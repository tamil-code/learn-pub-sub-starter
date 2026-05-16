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

var gameState *gamelogic.GameState

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		if gs == nil {
			return
		}
		gs.HandlePause(ps)
	}
}

func handlerArmyMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(am gamelogic.ArmyMove) {
		if gs == nil {
			return
		}
		gs.HandleMove(am)
	}
}

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
	gameState = gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, username, routing.PauseKey, pubsub.SimpleQueueTypeTransient, handlerPause(gameState))

	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"_"+username, routing.ArmyMovesKey, pubsub.SimpleQueueTypeTransient, handlerArmyMove(gameState))
	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
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

		if command == "spawn" {
			err := gameState.CommandSpawn(words[0:])
			if err != nil {
				fmt.Println("Failed to spawn unit:", err)
				continue
			}
		} else if command == "move" {
			move, err := gameState.CommandMove(words[0:])
			if err != nil {
				fmt.Println("Failed to move units:", err)
				continue
			}
			pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesKey, move)

			fmt.Println("Move was successfully published to other")
			fmt.Printf("Moved units: %+v\n", move)
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

	// fmt.Println("Declared and bound queue:", queue.Name)
	defer ch.Close()
	defer conn.Close()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down Peril client...")
	fmt.Println("Connected to RabbitMQ")

}
