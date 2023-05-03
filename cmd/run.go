package main

import (
	_ "github.com/Kirov7/CouloyDB/cmd/cluster"
	"github.com/Kirov7/CouloyDB/cmd/root"
	_ "github.com/Kirov7/CouloyDB/cmd/standalone"
)

func main() {
	root.Execute()
}
