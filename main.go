package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

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

	oldPackage := fileNames[0]
	go untar(ch, oldPackage, oldPackageDirName)

	newPackage := fileNames[1]
	go untar(ch, newPackage, newPackageDirName)

	// channels are not really needed here because we are not passing the values from the goroutines,
	// but anyway, we're unpacking the archives in concurrency mode though
	_, _ = <-ch, <-ch

	// processing the files
	log.Println("Reading the packages and generating the updates report")
	updates()

	// final report
	log.Println("Generating the final report")
	report()
}

func updates() {
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
			json.Unmarshal(s2.Bytes(), &entry)

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
			json.Unmarshal(s1.Bytes(), &entry)

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

func report() {
	var report = make(map[string]map[string]map[string][]interface{})

	newFiles := utils.FilesFromDirectory(newPackageDirName, "")
	if len(newFiles) == 0 {
		log.Println("No valid files found")

		return
	}
	oldFiles := utils.FilesFromDirectory(oldPackageDirName, "")
	if len(oldFiles) == 0 {
		log.Println("No valid files found")

		return
	}
	fmt.Println(oldPackageDirName, oldFiles)
	// Files level diff
	removedFiles, addedFiles := utils.SlicesDiff(oldFiles, newFiles)
	// fmt.Printf("\n\nRemoved files: %+v\n\nAdded files: %+v", removedFiles, addedFiles)

	// Types/Fields diff
	dictNewFiles := utils.FilesFromDirectory(newPackageDirName, "dict[A-z]+\\.json")
	dictOldFiles := utils.FilesFromDirectory(oldPackageDirName, "dict[A-z]+\\.json")

	for _, of := range dictOldFiles {
		var reportName = strings.Replace(of, ".json", "", -1)
		report[reportName] = map[string]map[string][]interface{}{
			"fields added":     map[string][]interface{}{},
			"fields removed":   map[string][]interface{}{},
			"changes detected": map[string][]interface{}{},
		}

		for _, nf := range dictNewFiles {
			if of == nf {
				// opening the files
				oldRaw, err := ioutil.ReadFile(oldPackageDirName + "/" + of)
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
				}
				var oldJSON map[string]interface{}
				json.Unmarshal(oldRaw, &oldJSON)

				newRaw, err := ioutil.ReadFile(newPackageDirName + "/" + nf)
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
				}
				var newJSON map[string]interface{}
				json.Unmarshal(newRaw, &newJSON)

				for ok, ov := range oldJSON {
					found := false

					// convert interface to map
					om := ov.(map[string]interface{})

					for nk, nv := range newJSON {
						// convert interface to map
						nm := nv.(map[string]interface{})

						// if the same field
						if ok == nk {
							found = true

							delete(newJSON, nk)

							if om["type"] != nm["type"] {
								report[reportName]["changes detected"][ok] = append(report[reportName]["changes detected"][ok], map[string]string{
									"from": om["type"].(string),
									"to":   nm["type"].(string)})
							}

							break
						}
					}

					// if no match = removed fields
					if !found {
						rm := map[string]interface{}{ok: om}
						// removedFields[of] = append(removedFields[of], rm)

						report[reportName]["fields removed"][of] = append(report[reportName]["fields removed"][of], rm)
					}
				}

				// all left are actually been added since
				for njk, njv := range newJSON {
					rm := map[string]interface{}{njk: njv}
					// addedFields[of] = append(addedFields[of], rm)

					report[reportName]["fields added"][of] = append(report[reportName]["fields added"][of], rm)
				}
			}
		}
	}

	for reportDict, dictValue := range report {
		fmt.Printf("\n\n-> %+v", reportDict)

		for diffType, value := range dictValue {
			changes := len(value)

			fmt.Printf("\n%d %+v", changes, diffType)

			for i, v := range value {
				switch diffType {
				case "changes detected":
					mv := v[0].(map[string]string)

					fmt.Printf("\n- %+v (from: `%v` to: `%v`)", i, mv["from"], mv["to"])
					break
				default:
					fmt.Printf("\n- %+v", i)
					break
				}

			}
		}
	}

	if len(removedFiles) > 0 {
		fmt.Printf("\n\n-> %+v", "Removed files")

		for _, file := range removedFiles {
			fmt.Printf("\n- %+v", file)
		}
	}
	if len(addedFiles) > 0 {
		fmt.Printf("\n\n-> %+v", "Added files")

		for _, file := range addedFiles {
			fmt.Printf("\n- %+v", file)
		}
	}

	c := utils.AskForConfirmation("Do you want to generate the report?")
	if !c {
		fmt.Println("\n\nFinished without generating the report.")
		return
	}

	//generate the report here

	// create report file
	f, _ := os.Create("report.diff")
	var w *bufio.Writer
	w = bufio.NewWriter(f)
	w.WriteString("Final package report\n")
	defer w.Flush()
	defer f.Close()

	fmt.Println()
	log.Println("Report generated")
}

// func test() {
// 	// opening an old package
// 	old, err := os.Open(oldPackageDirName + "/3.json")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer old.Close()

// 	mapping := map[string]interface{}{}

// 	s1 := bufio.NewScanner(old)
// 	for s1.Scan() {
// 		var f interface{}
// 		json.Unmarshal(s1.Bytes(), &f)

// 		line, ok := f.(map[string]interface{})
// 		if !ok {
// 			panic("Problem converting JSON to MAP")
// 		}
// 		for field, value := range line {
// 			fmt.Printf("%v ", field)
// 			checktype(value, field, mapping)
// 		}
// 		fmt.Printf("\n%+v\n", mapping)
// 		return
// 		// var entry Entry

// 		// // converting a json line to a struct
// 		// json.Unmarshal([]byte(s1.Text()), &entry)
// 	}
// }

// func checktype(in interface{}, f string, mapping map[string]interface{}) (typ string) {
// 	switch t := in.(type) {
// 	case int:
// 		typ = "integer"

// 		fmt.Printf("Integer: %v\n", t)
// 		break
// 	case float64:
// 		if t == float64(int64(t)) {
// 			typ = "integer"

// 			fmt.Printf("INT: %v\n", int(t))
// 		} else {
// 			typ = "float"

// 			fmt.Printf("Float64: %v\n", t)
// 		}
// 		break
// 	case string:
// 		typ = "string"

// 		fmt.Printf("String: %v\n", t)
// 		break
// 	case bool:
// 		typ = "boolean"

// 		fmt.Printf("Bool: %v\n", t)
// 		break
// 	case []string:
// 		// fmt.Printf("Array: %v\n", t)
// 		// case []map[int]interface{}:
// 		// 	fmt.Printf("Array: %v\n", t)
// 		// 	for _, n := range t {
// 		// 		checktype(n)
// 		// 	}
// 		break
// 	case map[string]interface{}:
// 		typ = "assocArray"

// 		fmt.Println("AssocArray: ")
// 		// for field, value := range t {
// 		// fmt.Printf("field => %+v ", field)
// 		// checktype(value, field, mapping[f][field])
// 		// }
// 		break
// 	case []interface{}:
// 		typ = "array"

// 		fmt.Printf("Array: ")
// 		// for _, n := range t {
// 		// 	checktype(n)
// 		// }
// 		break
// 	default:
// 		typ = "other"

// 		// var r = reflect.TypeOf(in)
// 		// fmt.Printf("Other:%v\n", r)
// 		break
// 	}

// 	// in["test"] = 1

// 	return typ
// }

func untar(c chan bool, archive string, target string) {
	if err := utils.Untar(archive, target); err != nil {
		panic(err)
	}

	// in this case I don't really need to return anything here, so flag is sent instead
	c <- true
}
