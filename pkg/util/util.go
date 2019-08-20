package util

import (
	"bufio"
	"os"
	"strings"
)

// ReadFile read a file as a 2d string array
func ReadFile(path string) ([][]string, error) {
	var content [][]string
	var err error

	f, err := os.Open(path)
	if err != nil {
		return content, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		content = append(content, strings.Split(scanner.Text(), " "))
	}

	return content, nil
}

// WriteFile write a 2d string array into a file
func WriteFile(path string, content [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, c := range content {
		line := strings.Join(c, " ")
		w.WriteString(line + "\n")
	}

	w.Flush()
	return nil
}
