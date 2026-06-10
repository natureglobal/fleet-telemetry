package sqs

import (
	"encoding/base64"
	"fmt"
	"maps"
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

// Producer client to handle sqs interactions
type Producer struct {
	sqs                *awssqs.SQS
	queueURL           string
	staticAttributes   map[string]*awssqs.MessageAttributeValue
	logger             *logrus.Logger
	prometheusEnabled  bool
	metricsCollector   metrics.MetricCollector
	airbrakeHandler    *airbrake.Handler
	ackChan            chan (*telemetry.Record)
	reliableAckTxTypes map[string]any
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
func NewProducer(queueURL, region, overrideHost string, messageAttributes map[string]string, prometheusEnabled bool, metricsCollector metrics.MetricCollector, airbrakeHandler *airbrake.Handler, ackChan chan (*telemetry.Record), reliableAckTxTypes map[string]interface{}, logger *logrus.Logger) (telemetry.Producer, error) {
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

	staticAttributes := make(map[string]*awssqs.MessageAttributeValue, len(messageAttributes))
	for k, v := range messageAttributes {
		if v == "" {
			continue
		}
		staticAttributes[k] = &awssqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	return &Producer{
		sqs:                service,
		queueURL:           queueURL,
		staticAttributes:   staticAttributes,
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
		MessageAttributes: p.messageAttributesFromRecord(entry),
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

// messageAttributesFromRecord merges the producer's configured static
// attributes with per-record metadata. Record metadata wins on key collision.
func (p *Producer) messageAttributesFromRecord(entry *telemetry.Record) map[string]*awssqs.MessageAttributeValue {
	attrs := make(map[string]*awssqs.MessageAttributeValue, len(p.staticAttributes)+len(entry.Metadata()))
	maps.Copy(attrs, p.staticAttributes)
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
