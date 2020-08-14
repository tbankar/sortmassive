package main

import (
        "math/rand"
        "os"
        "fmt"
)

// Main function
func main() {

        // Finding random numbers of int type
        // Using Int() function
        fp, _ := os.Create("input.txt")
        defer fp.Close()
        for i := 0; i < 1000000; i++ {
                fp.WriteString(fmt.Sprintf("%d\n", rand.Int()))
        }

}

