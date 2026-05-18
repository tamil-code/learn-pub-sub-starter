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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Println("> ")
		gs.HandlePause(ps)
		return pubsub.ACK
	}
}

func handlerArmyMove(gs *gamelogic.GameState, ch *amqp.Channel, username string) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Println("> ")
		switch gs.HandleMove(am) {
		case gamelogic.MoveOutcomeSamePlayer:
			fmt.Println("NACKDiscard")
			return pubsub.NACKDiscard
		case gamelogic.MoveOutComeSafe:
			fmt.Println("NACK of MoveOutComeSafe")
			return pubsub.ACK
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+username, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NACKRequeue
			}
			fmt.Println("ACK for MoveOutcomeMakeWar")
			return pubsub.ACK
		}
		fmt.Println("unknown outcome")
		return pubsub.NACKDiscard
	}

}

func handleWar(gs *gamelogic.GameState) func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, _, _ := gs.HandleWar(dw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NACKRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NACKDiscard
		case gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeDraw:
			return pubsub.ACK
		}
		fmt.Println("War outcome unknown")
		return pubsub.NACKDiscard
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
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"_"+username, routing.ArmyMovesKey, pubsub.SimpleQueueTypeTransient, handlerArmyMove(gameState, ch, username))
	if err != nil {
		fmt.Println("Failed to subscribe to queue:", err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.SimpleQueueTypeDurable, handleWar(gameState))
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
