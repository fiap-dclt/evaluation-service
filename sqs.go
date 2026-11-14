package main

import (
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a fila SQS
func (a *App) sendEvaluationEvent(ctx context.Context, userID, flagName string, result bool) {
	// Se a URL da fila não foi configurada, apenas loga localmente e sai.
	if a.SqsSvc == nil || a.SqsQueueURL == "" {
		log.Printf("[SQS_DISABLED] Evento não publicado (Flag: %s, Result: %t)", flagName, result)
		return
	}

	event := EvaluationEvent{
		UserID:    userID,
		FlagName:  flagName,
		Result:    result,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Erro ao serializar evento SQS: %v", err)
		return
	}

    // Envio assíncrono com prazo próprio, mantendo a propagação do trace.
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    // Preserva o traceparent nos atributos da mensagem, sem incluir credenciais.
    ctx, span := otel.Tracer("evaluation-service").Start(ctx, "sqs.send")
    defer span.End()
    carrier := propagation.MapCarrier{}
    otel.GetTextMapPropagator().Inject(ctx, carrier)
    attrs := make(map[string]*sqs.MessageAttributeValue)
    for _, key := range []string{"traceparent", "tracestate"} {
        if value := carrier.Get(key); value != "" {
            attrs[key] = &sqs.MessageAttributeValue{
                DataType: aws.String("String"),
                StringValue: aws.String(value),
            }
        }
    }

    // Envia a mensagem com o contexto, mantendo o trace distribuído.
    _, err = a.SqsSvc.SendMessageWithContext(ctx, &sqs.SendMessageInput{
        MessageAttributes: attrs,
		MessageBody: aws.String(string(body)),
		QueueUrl:    aws.String(a.SqsQueueURL),
	})

	if err != nil {
		log.Printf("Erro ao enviar mensagem para SQS: %v", err)
	} else {
		log.Printf("Evento de avaliação enviado para SQS (Flag: %s)", flagName)
	}
}
