package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

func handleWriteLog() func(gs routing.GameLog) pubsub.AckType {
	return func(gs routing.GameLog) pubsub.AckType {
		err := gamelogic.WriteLog(gs)
		if err != nil {
			fmt.Println("Failed to write log:", err)
			return pubsub.NACKRequeue
		}
		fmt.Print("Successfully written gamelog")
		return pubsub.ACK
	}
}
func main() {
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(RABBITMQ_URL)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}

	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")

	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.SimpleQueueTypeDurable)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}

	fmt.Println("Declared and bound queue:", queue.Name)
	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.SimpleQueueTypeDurable, handleWriteLog())
	if err != nil {
		fmt.Println("Failed to subscribe gob gamelog queue:", err)
		return
	}
	for {
		gamelogic.PrintServerHelp()
		words := gamelogic.GetInput()
		if len(words) == 0 {
			fmt.Println("You must enter a command. goodbye")
			continue
		}
		command := words[0]

		if command == "pause" {
			fmt.Println("Pausing game...")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		} else if command == "resume" {
			fmt.Println("Resuming game...")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		} else if command == "quit" {
			fmt.Println("Goodbye!")
			break
		} else if command == "help" {
			gamelogic.PrintServerHelp()
		} else {
			fmt.Println("Invalid command. goodbye")
			break
		}

	}
	defer ch.Close()

	pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down Peril server...")
}
