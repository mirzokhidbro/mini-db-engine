package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"rdbms/src"
	"rdbms/src/sql"
	"strings"
)

type CLI struct {
	executor *sql.Executor
}

func NewCLI(stg src.StorageI) *CLI {
	return &CLI{
		executor: sql.NewExecutor(stg),
	}
}

func (c *CLI) Run() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("RDBMS CLI v1.0")
	fmt.Println("Type 'exit' to quit, 'help' for commands")
	fmt.Println()

	for {
		fmt.Print("rdbms> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\nGoodbye!")
				break
			}
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		if strings.ToLower(input) == "help" {
			c.printHelp()
			continue
		}

		result, err := c.executor.Execute(input)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			continue
		}

		c.printResult(result)
	}
}

func (c *CLI) printHelp() {
	fmt.Println("Supported SQL commands:")
	fmt.Println("  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)")
	fmt.Println("  INSERT INTO name (col1, col2) VALUES (val1, val2)")
	fmt.Println("  SELECT col1, col2 FROM name WHERE condition")
	fmt.Println("  UPDATE name SET col1 = val1 WHERE condition")
	fmt.Println("  DELETE FROM name WHERE condition")
	fmt.Println()
	fmt.Println("Supported types: INT, VARCHAR(n), FLOAT, DATE, TIMESTAMP, JSON")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  help  - Show this help")
	fmt.Println("  exit  - Exit CLI")
	fmt.Println()
}

func (c *CLI) printResult(result *sql.Result) {
	switch result.Type {
	case "SELECT":
		if len(result.Data) == 0 {
			fmt.Println("(0 rows)")
			return
		}

		if len(result.Data) > 0 {
			var headers []string
			for key := range result.Data[0] {
				headers = append(headers, key)
			}

			colWidths := make(map[string]int)
			for _, h := range headers {
				colWidths[h] = len(h)
			}
			for _, row := range result.Data {
				for _, h := range headers {
					valStr := fmt.Sprintf("%v", row[h])
					if len(valStr) > colWidths[h] {
						colWidths[h] = len(valStr)
					}
				}
			}

			headerLine := "|"
			separator := "+"
			for _, h := range headers {
				headerLine += fmt.Sprintf(" %-*s |", colWidths[h], h)
				separator += strings.Repeat("-", colWidths[h]+2) + "+"
			}
			fmt.Println(headerLine)
			fmt.Println(separator)

			for _, row := range result.Data {
				rowLine := "|"
				for _, h := range headers {
					rowLine += fmt.Sprintf(" %-*v |", colWidths[h], row[h])
				}
				fmt.Println(rowLine)
			}
			fmt.Printf("(%d rows)\n", len(result.Data))
		}

	case "INSERT", "UPDATE", "DELETE":
		fmt.Println(result.Message)

	case "CREATE":
		fmt.Println(result.Message)

	default:
		fmt.Println(result.Message)
	}
}
