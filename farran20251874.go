package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	// Read numbers from a CSV file.
	numbers, err := readNumbers("in.csv")
	if err != nil {
		log.Fatalf("Error reading numbers: %v", err)
	}

	// Start timing the sorting process.
	start := time.Now()

	// Sort the numbers using introsort.
	introsort(numbers)

	// Stop timing and calculate elapsed time.
	elapsed := time.Since(start)

	// Write the sorted numbers to a new CSV file.
	err = writeNumbers("out20251874.csv", numbers)
	if err != nil {
		log.Fatalf("Error writing numbers: %v", err)
	}

	// Print out the number of sorted numbers and the time taken.
	fmt.Printf("Sorted %d numbers in %s.\n", len(numbers), elapsed)

	// Check if the numbers are sorted correctly and print the result.
	if isSorted(numbers) {
		fmt.Println("The numbers are sorted correctly.")
	} else {
		fmt.Println("The numbers are not sorted correctly.")
	}
}

// readNumbers reads integers from a CSV file and returns them as a slice.
func readNumbers(filename string) ([]int, error) {
	// Open the input CSV file.
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a new reader for the CSV file.
	reader := csv.NewReader(file)

	// Slice to store the numbers.
	numbers := []int{}
	for {
		// Read a record (line) from the CSV file.
		record, err := reader.Read()
		if err == io.EOF {
			break // End of file reached.
		}
		if err != nil {
			return nil, err
		}

		// Convert each value in the record to an integer and append it to the slice.
		for _, value := range record {
			number, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			numbers = append(numbers, number)
		}
	}

	return numbers, nil
}

// writeNumbers writes a slice of integers to a CSV file.
func writeNumbers(filename string, numbers []int) error {
	// Create and open the output CSV file.
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a writer for the CSV file.
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write each number in the slice to the CSV file.
	for _, number := range numbers {
		err := writer.Write([]string{strconv.Itoa(number)})
		if err != nil {
			return err
		}
	}

	return nil
}

// isSorted checks if a slice of integers is sorted in ascending order.
func isSorted(numbers []int) bool {
	for i := 1; i < len(numbers); i++ {
		if numbers[i-1] > numbers[i] {
			return false // Found an element out of order.
		}
	}
	return true
}

// partition is a helper function for quicksort that partitions the array around a pivot.
func partition(a []int, low, high int) int {
	// Use the median of three as the pivot for improved performance.
	median := medianOfThree(a, low, high)
	a[median], a[high] = a[high], a[median]

	// Standard partitioning logic.
	pivot := a[high]
	i := low - 1
	for j := low; j < high; j++ {
		if a[j] < pivot {
			i++
			a[i], a[j] = a[j], a[i]
		}
	}
	a[i+1], a[high] = a[high], a[i+1]
	return i + 1
}

// medianOfThree chooses the median of the first, middle, and last elements.
func medianOfThree(a []int, low, high int) int {
	mid := low + (high-low)/2
	if a[mid] < a[low] {
		a[mid], a[low] = a[low], a[mid]
	}
	if a[high] < a[low] {
		a[high], a[low] = a[low], a[high]
	}
	if a[mid] < a[high] {
		a[mid], a[high] = a[high], a[mid]
	}
	return high
}

func insertionSort(a []int) {
	for i := 1; i < len(a); i++ {
		key := a[i] // The element to be positioned
		j := i - 1

		// Move elements that are greater than key to one position ahead of their current position
		for j >= 0 && a[j] > key {
			a[j+1] = a[j]
			j = j - 1
		}
		a[j+1] = key // Place key at after the element just smaller than it
	}
}

// quicksort is an implementation of the QuickSort algorithm with a depth limit for optimization.
func quicksort(a []int, low, high, depthLimit int) {
	if low < high {
		// Use insertion sort for small subarrays for better performance
		if high-low <= 10 {
			insertionSort(a[low : high+1])
			return
		}

		// Recursively sort the elements before and after partition
		if depthLimit == 0 {
			// Switch to heapSortParallel when the depth limit is reached
			heapSortParallel(a[low : high+1])
			return
		}
		pi := partition(a, low, high)          // Partition the array
		quicksort(a, low, pi-1, depthLimit-1)  // Sort the elements before the partition
		quicksort(a, pi+1, high, depthLimit-1) // Sort the elements after the partition
	}
}

// heapify turns a subtree into a max heap, used in heap sort.
func heapify(a []int, n, i int) {
	largest := i // Initialize largest as root
	l := 2*i + 1 // left child
	r := 2*i + 2 // right child

	// If left child is larger than root
	if l < n && a[l] > a[largest] {
		largest = l
	}
	// If right child is larger than largest so far
	if r < n && a[r] > a[largest] {
		largest = r
	}

	// If largest is not root, swap and continue heapifying
	if largest != i {
		a[i], a[largest] = a[largest], a[i]
		heapify(a, n, largest)
	}
}

// heapifyParallel is the parallel version of the heapify function.
func heapifyParallel(a []int, n, i int, wg *sync.WaitGroup) {
	defer wg.Done() // Signal done when the function exits

	largest := i
	l := 2*i + 1 // left child
	r := 2*i + 2 // right child

	// Same as heapify, but starts new goroutines for recursive calls
	if l < n && a[l] > a[largest] {
		largest = l
	}
	if r < n && a[r] > a[largest] {
		largest = r
	}

	if largest != i {
		a[i], a[largest] = a[largest], a[i]
		wg.Add(1) // Add a new task to the wait group
		go heapifyParallel(a, n, largest, wg)
	}
}

// heapSortParallel sorts an array using the heap sort algorithm in parallel.
func heapSortParallel(a []int) {
	n := len(a)
	var wg sync.WaitGroup // A WaitGroup waits for a collection of goroutines to finish

	for i := n/2 - 1; i >= 0; i-- {
		// Use parallel heapify for large subarrays
		if n > 20 {
			wg.Add(1)
			go heapifyParallel(a, n, i, &wg)
		} else {
			heapify(a, n, i) // Use standard heapify for smaller subarrays
		}
	}
	wg.Wait() // Wait for all heapify operations to finish

	for i := n - 1; i >= 0; i-- {
		// Move current root to end
		a[0], a[i] = a[i], a[0]

		// Call max heapify on the reduced heap
		if i > 20 {
			wg.Add(1)
			go heapifyParallel(a, i, 0, &wg)
			wg.Wait()
		} else {
			heapify(a, i, 0)
		}
	}
}

func introsort(a []int) {
	maxDepth := int(math.Log2(float64(len(a)))) * 2
	quicksort(a, 0, len(a)-1, maxDepth)
}
