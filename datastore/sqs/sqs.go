package sqs

import (
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/metrics"
	"github.com/teslamotors/fleet-telemetry/metrics/adapter"
	"github.com/teslamotors/fleet-telemetry/server/airbrake"
	"github.com/teslamotors/fleet-telemetry/telemetry"
)

const (
	// JobTypeAttributeName is the MessageAttribute key consumers route on
	// when the same queue carries multiple job types.
	JobTypeAttributeName = "type"
	// JobTypeTeslaTelemetry is the JobTypeAttributeName value set on every
	// message produced by this package.
	JobTypeTeslaTelemetry = "tesla_telemetry"
)

// Producer client to handle sqs interactions
type Producer struct {
	sqs                *awssqs.SQS
	queueURL           string
	logger             *logrus.Logger
	prometheusEnabled  bool
	metricsCollector   metrics.MetricCollector
	airbrakeHandler    *airbrake.Handler
	ackChan            chan (*telemetry.Record)
	reliableAckTxTypes map[string]interface{}
}

// Metrics stores metrics reported from this package
type Metrics struct {
	errorCount       adapter.Counter
	publishCount     adapter.Counter
	byteTotal        adapter.Counter
	reliableAckCount adapter.Counter
}

var (
	metricsRegistry Metrics
	metricsOnce     sync.Once
)

// NewProducer configures and tests the sqs connection
func NewProducer(queueURL, region, overrideHost string, prometheusEnabled bool, metricsCollector metrics.MetricCollector, airbrakeHandler *airbrake.Handler, ackChan chan (*telemetry.Record), reliableAckTxTypes map[string]interface{}, logger *logrus.Logger) (telemetry.Producer, error) {
	registerMetricsOnce(metricsCollector)

	if queueURL == "" {
		return nil, fmt.Errorf("sqs queue_url is required")
	}

	awsConfig := &aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
	}
	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}
	if overrideHost != "" {
		awsConfig = awsConfig.WithEndpoint(overrideHost)
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            *awsConfig,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	service := awssqs.New(sess, awsConfig)
	if _, err := service.ListQueues(&awssqs.ListQueuesInput{MaxResults: aws.Int64(1)}); err != nil {
		return nil, fmt.Errorf("failed to list queues (test connection): %v", err)
	}

	return &Producer{
		sqs:                service,
		queueURL:           queueURL,
		logger:             logger,
		prometheusEnabled:  prometheusEnabled,
		metricsCollector:   metricsCollector,
		airbrakeHandler:    airbrakeHandler,
		ackChan:            ackChan,
		reliableAckTxTypes: reliableAckTxTypes,
	}, nil
}

// Produce sends the record payload to sqs (base64-encoded for UTF-8 body)
func (p *Producer) Produce(entry *telemetry.Record) {
	entry.ProduceTime = time.Now()

	body := base64.StdEncoding.EncodeToString(entry.Payload())

	out, err := p.sqs.SendMessage(&awssqs.SendMessageInput{
		QueueUrl:          aws.String(p.queueURL),
		MessageBody:       aws.String(body),
		MessageAttributes: messageAttributesFromRecord(entry),
	})
	if err != nil {
		p.ReportError("sqs_err", err, logrus.LogInfo{"record_type": entry.TxType})
		metricsRegistry.errorCount.Inc(map[string]string{"record_type": entry.TxType})
		return
	}

	p.ProcessReliableAck(entry)
	p.logger.Log(logrus.DEBUG, "sqs_message_dispatched", logrus.LogInfo{
		"vin":         entry.Vin,
		"record_type": entry.TxType,
		"txid":        entry.Txid,
		"message_id":  aws.StringValue(out.MessageId),
	})
	metricsRegistry.publishCount.Inc(map[string]string{"record_type": entry.TxType})
	metricsRegistry.byteTotal.Add(int64(entry.Length()), map[string]string{"record_type": entry.TxType})
}

// messageAttributesFromRecord converts record metadata to sqs message attributes
func messageAttributesFromRecord(entry *telemetry.Record) map[string]*awssqs.MessageAttributeValue {
	attrs := map[string]*awssqs.MessageAttributeValue{
		JobTypeAttributeName: {
			DataType:    aws.String("String"),
			StringValue: aws.String(JobTypeTeslaTelemetry),
		},
	}
	for k, v := range entry.Metadata() {
		// sqs rejects message attributes with empty StringValue
		if v == "" {
			continue
		}
		attrs[k] = &awssqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return attrs
}

// Close the producer
func (p *Producer) Close() error {
	return nil
}

// ProcessReliableAck sends to ackChan if reliable ack is configured
func (p *Producer) ProcessReliableAck(entry *telemetry.Record) {
	if _, ok := p.reliableAckTxTypes[entry.TxType]; ok {
		p.ackChan <- entry
		metricsRegistry.reliableAckCount.Inc(map[string]string{"record_type": entry.TxType})
	}
}

// ReportError to airbrake and logger
func (p *Producer) ReportError(message string, err error, logInfo logrus.LogInfo) {
	p.airbrakeHandler.ReportLogMessage(logrus.ERROR, message, err, logInfo)
	p.logger.ErrorLog(message, err, logInfo)
}

func registerMetricsOnce(metricsCollector metrics.MetricCollector) {
	metricsOnce.Do(func() { registerMetrics(metricsCollector) })
}

func registerMetrics(metricsCollector metrics.MetricCollector) {
	metricsRegistry.errorCount = metricsCollector.RegisterCounter(adapter.CollectorOptions{
		Name:   "sqs_err",
		Help:   "The number of errors while producing to SQS.",
		Labels: []string{"record_type"},
	})

	metricsRegistry.publishCount = metricsCollector.RegisterCounter(adapter.CollectorOptions{
		Name:   "sqs_publish_total",
		Help:   "The number of messages published to SQS.",
		Labels: []string{"record_type"},
	})

	metricsRegistry.byteTotal = metricsCollector.RegisterCounter(adapter.CollectorOptions{
		Name:   "sqs_publish_total_bytes",
		Help:   "The number of bytes published to SQS.",
		Labels: []string{"record_type"},
	})

	metricsRegistry.reliableAckCount = metricsCollector.RegisterCounter(adapter.CollectorOptions{
		Name:   "sqs_reliable_ack_total",
		Help:   "The number of records produced to SQS for which we sent a reliable ACK.",
		Labels: []string{"record_type"},
	})
}
