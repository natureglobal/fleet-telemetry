package integration_test

import (
	"encoding/base64"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// TestSQSConsumer is a thin wrapper around the SQS client used by the
// integration suite. Mirrors TestKinesisConsumer.
type TestSQSConsumer struct {
	sqs      *sqs.SQS
	queueURL string
}

// NewTestSQSConsumer creates the queue (if needed) and returns a consumer
// bound to it. queueName is the FIFO/standard queue name (without the URL
// prefix); the URL is resolved from the local SQS endpoint.
func NewTestSQSConsumer(host, queueName string) (*TestSQSConsumer, error) {
	creds := credentials.NewStaticCredentials(fakeAWSID, fakeAWSSecret, fakeAWSToken)
	awsConfig := aws.NewConfig().WithEndpoint(host).WithCredentialsChainVerboseErrors(true).WithRegion(fakeAWSRegion).WithCredentials(creds)
	sess, err := session.NewSessionWithOptions(session.Options{Config: *awsConfig})
	if err != nil {
		return nil, err
	}

	t := &TestSQSConsumer{sqs: sqs.New(sess, awsConfig)}
	url, err := t.createQueueIfNotExists(queueName)
	if err != nil {
		return nil, err
	}
	t.queueURL = url
	return t, nil
}

func (t *TestSQSConsumer) createQueueIfNotExists(queueName string) (string, error) {
	out, err := t.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err == nil {
		return aws.StringValue(out.QueueUrl), nil
	}
	if !strings.Contains(err.Error(), "NonExistentQueue") {
		return "", err
	}
	created, err := t.sqs.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		return "", err
	}
	return aws.StringValue(created.QueueUrl), nil
}

// QueueURL returns the resolved queue URL.
func (t *TestSQSConsumer) QueueURL() string { return t.queueURL }

// FetchFirstMessage long-polls for a single message. Returns the decoded
// message body (base64 → raw bytes) along with its MessageAttributes so
// callers can assert routing labels.
func (t *TestSQSConsumer) FetchFirstMessage() ([]byte, map[string]string, error) {
	out, err := t.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(t.queueURL),
		MaxNumberOfMessages:   aws.Int64(1),
		WaitTimeSeconds:       aws.Int64(1),
		MessageAttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		return nil, nil, err
	}
	if len(out.Messages) == 0 {
		return nil, nil, errors.New("empty messages")
	}
	msg := out.Messages[0]
	raw, err := base64.StdEncoding.DecodeString(aws.StringValue(msg.Body))
	if err != nil {
		return nil, nil, err
	}
	attrs := map[string]string{}
	for k, v := range msg.MessageAttributes {
		attrs[k] = aws.StringValue(v.StringValue)
	}
	// Best-effort cleanup so subsequent polls in other tests start clean.
	_, _ = t.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(t.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return raw, attrs, nil
}
