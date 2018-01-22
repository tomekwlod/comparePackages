package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/tomekwlod/utils"
)

type Entry struct {
	ID          int      `json:"id"`
	Npi         int      `json:"npi"`
	TTID        int      `json:"ttid"`
	FirstName   int      `json:"first_name"`
	MiddleName  int      `json:"middle_name"`
	LastName    int      `json:"last_name"`
	Specialties []string `json:"specialties"`
	LocationID  int      `json:"location.location"`
	Position    int      `json:"ranking.position"`
}

const oldPackageDirName = "oldPackage"
const newPackageDirName = "newPackage"

func main() {
	fileNames := os.Args[1:]

	if len(fileNames) < 2 || len(fileNames) > 2 {
		panic("you must pass exactly 2 arguments, like: `command oldPackage.tar.gz newPackage.tar.gz`")
	}

	log.Println("Unpacking files")

	// initializing the channel
	ch := make(chan bool)

	oldPackageFile := fileNames[0]
	go untar(ch, oldPackageFile, oldPackageDirName)

	newPackageFile := fileNames[1]
	go untar(ch, newPackageFile, newPackageDirName)

	// channels are not really needed here because we are not passing the values from the goroutines,
	// but anyway, we're unpacking the archives in concurrency mode though
	_, _ = <-ch, <-ch

	log.Println("Unpacking done")

	// processing the files
	log.Println("Reading the packages and generating the report")
	process()
}

func process() {
	files := utils.FilesFromDirectory(newPackageDirName, "[\\d]{1,2}.json")

	if len(files) == 0 {
		log.Println("No valid files found")

		return
	}

	// create report file
	f, _ := os.Create("updates.diff")
	var w *bufio.Writer
	w = bufio.NewWriter(f)
	defer w.Flush()
	defer f.Close()

	for _, file := range files {
		fmt.Println("")
		log.Println("Working on " + file)

		// opening an old package
		old, err := os.Open(oldPackageDirName + "/" + file)
		if err != nil {
			log.Fatal(err)
		}
		defer old.Close()

		// opening a new package
		new, err := os.Open(newPackageDirName + "/" + file)
		if err != nil {
			log.Fatal(err)
		}
		defer new.Close()

		// loading an old package into a memory
		previousEntries := map[int]Entry{}
		s2 := bufio.NewScanner(old)
		for s2.Scan() {
			var entry Entry
			json.Unmarshal([]byte(s2.Text()), &entry)

			previousEntries[entry.ID] = entry
		}
		if err := s2.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "An error occured:", err)
		}

		// walking line-by-line and comparing the new with the old package files
		s1 := bufio.NewScanner(new)
		for s1.Scan() {
			var entry Entry

			// converting a json line to a struct
			json.Unmarshal([]byte(s1.Text()), &entry)

			id := entry.ID

			if pe, ok := previousEntries[id]; !ok {
				// new entry detected!
				w.WriteString(strconv.Itoa(id) + "\n")
			} else {
				// entry found! let's check the differences

				if pe.FirstName != entry.FirstName {
					// fmt.Printf("First name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
				if pe.LastName != entry.LastName {
					// fmt.Printf("Last name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
				if pe.MiddleName != entry.MiddleName {
					// fmt.Printf("Middle name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
				if pe.LocationID != entry.LocationID {
					// fmt.Printf("Location changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
				if pe.Npi != entry.Npi {
					// fmt.Printf("NPI changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
				if pe.TTID != entry.TTID {
					// fmt.Printf("TTID changed: %d (%d != %d)\n", id, oldKOL.TTID, kol.TTID)
					w.WriteString(strconv.Itoa(id) + "\n")
				}
			}
		}
		if err := s1.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "An error occured:", err)
		}

		fmt.Println("")
	}
}

func untar(c chan bool, file1 string, file2 string) {
	if err := utils.Untar(file1, file2); err != nil {
		panic(err)
	}

	// in this case I don't really need to return anything here, so flag is sent instead
	c <- true
}
