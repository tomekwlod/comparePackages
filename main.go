package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tomekwlod/utils"
)

type entry struct {
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

	// benchmark start
	timeStart := time.Now()

	oldPackage := fileNames[0]
	newPackage := fileNames[1]

	log.Println("Unpacking files")

	// initializing the channel
	ch := make(chan bool)

	go untar(ch, oldPackage, oldPackageDirName)
	go untar(ch, newPackage, newPackageDirName)

	// channels are not really needed here because we are not passing the values from the goroutines,
	// but anyway, we're unpacking the archives in concurrency mode though
	_, _ = <-ch, <-ch

	// processing the files
	log.Println("Reading the packages and generating the updates report")
	updates(oldPackage, newPackage)

	// final report
	log.Println("Generating the final report")
	report(oldPackage, newPackage)

	// benchmark stop
	duration := time.Since(timeStart).Minutes()

	log.Println("All done in " + strconv.FormatFloat(duration, 'g', 1, 64) + " minutes")
}

func updates(oldPackage, newPackage string) {
	files := utils.FilesFromDirectory(newPackageDirName, "[\\d]{1,2}.json")

	if len(files) == 0 {
		log.Println("No valid files found")

		return
	}

	// create report file
	f, _ := os.Create("updates.diff")
	defer f.Close()
	var w *bufio.Writer
	w = bufio.NewWriter(f)
	defer w.Flush()

	w.WriteString("Update report (" + strings.Replace(filepath.Base(oldPackage), ".tar.gz", "", -1) + " - " + strings.Replace(filepath.Base(newPackage), ".tar.gz", "", -1) + ")\n")

	for _, file := range files {
		// fmt.Println("")
		// log.Println("Working on " + file)

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
		previousEntries := map[int]entry{}
		s2 := bufio.NewScanner(old)
		for s2.Scan() {
			var e entry
			json.Unmarshal(s2.Bytes(), &e)

			previousEntries[e.ID] = e
		}
		if err := s2.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "An error occured:", err)
		}

		// walking line-by-line and comparing the new with the old package files
		s1 := bufio.NewScanner(new)
		for s1.Scan() {
			var e entry

			// converting a json line to a struct
			json.Unmarshal(s1.Bytes(), &e)

			id := e.ID

			if pe, ok := previousEntries[id]; !ok {
				// new entry detected!
				w.WriteString(strconv.Itoa(id) + " \n")
			} else {
				// match found! let's check the differences

				delete(previousEntries, id)

				if pe.Npi != e.Npi {
					// fmt.Printf("NPI changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + " \n")
				} else if pe.TTID != e.TTID {
					// fmt.Printf("TTID changed: %d (%d != %d)\n", id, oldKOL.TTID, kol.TTID)
					w.WriteString(strconv.Itoa(id) + " \n")
				} else if pe.FirstName != e.FirstName {
					// fmt.Printf("First name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + " \n")
				} else if pe.LastName != e.LastName {
					// fmt.Printf("Last name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + " \n")
				} else if pe.MiddleName != e.MiddleName {
					// fmt.Printf("Middle name changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + " \n")
				} else if pe.LocationID != e.LocationID {
					// fmt.Printf("Location changed: %d\n", id)
					w.WriteString(strconv.Itoa(id) + " \n")
				}
			}

		}
		if err := s1.Err(); err != nil {
			panic(err)
		}

		for de := range previousEntries {
			w.WriteString(strconv.Itoa(de) + " \n")
		}
	}
}

func report(oldPackage, newPackage string) {
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

	// Files level diff
	removedFiles, addedFiles := utils.SlicesDiff(oldFiles, newFiles)

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

						report[reportName]["fields removed"][ok] = append(report[reportName]["fields removed"][ok], rm)
					}
				}

				// all left are actually been added since
				for njk, njv := range newJSON {
					rm := map[string]interface{}{njk: njv}

					report[reportName]["fields added"][njk] = append(report[reportName]["fields added"][njk], rm)
				}
			}
		}
	}

	// create report file
	f, _ := os.Create("report.diff")
	defer f.Close()
	var w *bufio.Writer
	w = bufio.NewWriter(f)
	defer w.Flush()

	w.WriteString("Final package report (" + strings.Replace(filepath.Base(oldPackage), ".tar.gz", "", -1) + " - " + strings.Replace(filepath.Base(newPackage), ".tar.gz", "", -1) + ")\n")

	for reportDict, dictValue := range report {
		// fmt.Printf("\n\n-> %+v", reportDict)
		w.WriteString("\n-> " + reportDict + "\n")

		for diffType, value := range dictValue {
			changes := len(value)

			// fmt.Printf("\n%d %+v", changes, diffType)
			w.WriteString(strconv.Itoa(changes) + " " + diffType + "\n")

			for i, v := range value {
				switch diffType {
				case "changes detected":
					mv := v[0].(map[string]string)

					// fmt.Printf("\n- %+v (from: `%v` to: `%v`)", i, mv["from"], mv["to"])
					w.WriteString("- " + i + " (from: `" + mv["from"] + "` to: `" + mv["to"] + "`)" + "\n")
					break
				default:
					// fmt.Printf("\n- %+v", i)
					w.WriteString("- " + i + "\n")
					break
				}

			}
		}
	}

	if len(removedFiles) > 0 {
		// fmt.Printf("\n\n-> %+v", "Removed files")
		w.WriteString("\n-> Removed files\n")

		for _, file := range removedFiles {
			// fmt.Printf("\n- %+v", file)
			w.WriteString("- " + file + "\n")
		}
	}
	if len(addedFiles) > 0 {
		// fmt.Printf("\n\n-> %+v", "Added files")
		w.WriteString("\n-> Added files\n")

		for _, file := range addedFiles {
			// fmt.Printf("\n- %+v", file)
			w.WriteString("- " + file + "\n")
		}
	}

	// // Confirmation
	// c := utils.AskForConfirmation("Do you want to generate the report?")
	// if !c {
	// 	fmt.Println("\n\nFinished without generating the report.")
	// 	return
	// }
	return
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
