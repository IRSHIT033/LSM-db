package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IRSHIT033/lsmdb"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "./config.yaml", "Path to the configuration file")
	flag.Parse()

	// Create database instance
	db, err := lsmdb.NewDB(*configPath)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create scanner for reading input
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("LSM Database CLI")
	fmt.Println("Commands:")
	fmt.Println("  set <key> <value> - Set a key-value pair")
	fmt.Println("  get <key>         - Get value for a key")
	fmt.Println("  del <key>         - Delete a key")
	fmt.Println("  exit              - Exit the CLI")
	fmt.Println()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		if input == "exit" {
			break
		}

		parts := strings.Fields(input)
		if len(parts) < 1 {
			continue
		}

		command := parts[0]
		switch command {
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			err := db.Put([]byte(parts[1]), []byte(parts[2]))
			if err != nil {
				fmt.Printf("Error setting key: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			value, err := db.Get([]byte(parts[1]))
			if err != nil {
				fmt.Printf("Error getting key: %v\n", err)
			} else if value == nil {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(value))
			}

		case "del":
			if len(parts) != 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			err := db.Delete([]byte(parts[1]))
			if err != nil {
				fmt.Printf("Error deleting key: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		default:
			fmt.Println("Unknown command. Available commands: set, get, del, exit")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}
