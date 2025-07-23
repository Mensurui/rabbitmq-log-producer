package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gofrs/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type AuditEventMessage struct {
	EventID     uuid.UUID       `json:"event_id"`
	Timestamp   time.Time       `json:"timestamp"`
	Action      string          `json:"action"`
	Status      string          `json:"status"`
	ActorID     string          `json:"actor_id"`
	ActorType   string          `json:"actor_type"`
	IPAddress   string          `json:"ip_address,omitempty"`
	UserAgent   string          `json:"user_agent,omitempty"`
	Resource    string          `json:"resource,omitempty"`
	ResourceID  string          `json:"resource_id,omitempty"`
	Details     json.RawMessage `json:"details,omitempty"` // Use json.RawMessage for flexibility
	ServiceName string          `json:"service_name"`
}

func main() {
	// RabbitMQ connection URL
	connectionURL := "amqp://mensur:mensur123@localhost:5672"

	conn, err := amqp.Dial(connectionURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare the exchange (must match the consumer's exchange declaration)
	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// List of services to send messages to (routing keys)
	services := []string{
		"document_service",
		"work_flow_service",
		"signature_service",
		"user_service_wrapper",
		"notification_service",
	}

	// --- Modified section to use AuditEventMessage ---

	// Create an example AuditEventMessage
	uuid, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	auditMessage := AuditEventMessage{
		EventID:     uuid,
		Timestamp:   time.Now(),
		Action:      "CREATE_DOCUMENT",
		Status:      "SUCCESS",
		ActorID:     "user-12345",
		ActorType:   "USER",
		IPAddress:   "192.168.1.100",
		UserAgent:   "Mozilla/5.0",
		Resource:    "Document",
		ResourceID:  "doc-abcde",
		ServiceName: "document_service", // This will be overridden by the loop below
		Details: json.RawMessage(`{
			"document_name": "report.pdf",
			"uploaded_by": "user-12345"
		}`),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, service := range services {
		// Update the ServiceName for each service this message is sent to
		auditMessage.ServiceName = service

		// Marshal the AuditEventMessage struct to JSON
		body, err := json.Marshal(auditMessage)
		failOnError(err, "Failed to marshal audit message")

		err = ch.PublishWithContext(
			ctx,
			"logs_topic", // exchange
			service,      // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "application/json", // Set content type to JSON
				Body:        body,
			})
		failOnError(err, "Failed to publish message")
		log.Printf(" [x] Sent AuditEventMessage to '%s': %+v", service, auditMessage)
	}

	log.Println("All AuditEventMessages published successfully.")
}
