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

func main() {
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(RABBITMQ_URL)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to welcome client:", err)
		return
	}
	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, username, routing.PauseKey, pubsub.SimpleQueueTypeTransient)

	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	for {
		gamelogic.PrintClientHelp()
		words := gamelogic.GetInput()
		if len(words) == 0 {
			fmt.Println("You must enter a command. goodbye")
			return
		}
		command := words[0]
		gameState := gamelogic.NewGameState(username)

		if command == "spawn" {
			gameState.CommandSpawn(words[1:])
		} else if command == "move" {
			gameState.CommandMove(words[1:])
		} else if command == "status" {
			gameState.CommandStatus()
		} else if command == "help" {
			gamelogic.PrintClientHelp()
		} else if command == "quit" {
			fmt.Println("Goodbye!")
			break
		} else if command == "spam" {
			fmt.Println("Spamming not allowed yet!")
		} else {
			fmt.Println("Invalid command. goodbye")
			continue
		}
	}

	fmt.Println("Declared and bound queue:", queue.Name)
	defer ch.Close()
	defer conn.Close()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down Peril client...")
	fmt.Println("Connected to RabbitMQ")

}
