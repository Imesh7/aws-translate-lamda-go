package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var queueUrl string = "https://sqs.us-east-1.amazonaws.com/992382396149/translate-new"

type Clients struct {
	sqsClient *sqs.Client
	s3Client  *s3.Client
}

type TranslateTextResponse struct {
	SourceType     string `json:"sourceType" bson:"sourceType"`
	OriginalText   string `json:"originalText" bson:"originalText"`
	TranslatedText string `json:"translatedText" bson:"translatedText"`
}

func NewClient(sqsClient *sqs.Client, s3Client *s3.Client) (Clients, error) {
	return Clients{
		sqsClient: sqsClient,
		s3Client:  s3Client,
	}, nil
}

// func (client *Clients) Handler(ctx context.Context, snsEvent events.SNSEvent) {
// 	var first map[string]TranslateTextResponse
// 	first = make(map[string]TranslateTextResponse)

// 	var keyMapList []TranslateTextResponse

// 	for _, val := range snsEvent.Records{
// 		msg, err := client.sqsClient.ReceiveMessage(ctx, &client.messageInput)
// 		if err != nil {
// 			panic(err)
// 		}

// 		json.Unmarshal([]byte(val.SNS.Message), &first)
// 		languageTo := first["text"].SourceType

// 		record, keyMap, allKeys, languageCol, rowLength, err := readFile(client, languageTo)
// 		if err != nil {
// 			panic(err)
// 		}

// 		var result map[string]TranslateTextResponse
// 		//send batch result
// 		for _, v := range msg.Messages {
// 			json.Unmarshal([]byte(*v.Body), &result)

// 			originalText := result["text"].OriginalText
// 			translatedText := result["text"].TranslatedText
// 			sourceType := result["text"].SourceType

// 			//if the key is not contains in the Map
// 			_, ok := keyMap[originalText]

// 			if !ok {
// 				//key is not in the csv
// 				keyMapList = append(keyMapList, TranslateTextResponse{OriginalText: originalText, TranslatedText: translatedText, SourceType: sourceType})
// 			}
// 		}
// 		writeToCsv(client.s3Client, keyMapList, record, allKeys, languageCol, rowLength)
// 	}
// }

func (client *Clients) Handler(ctx context.Context, snsEvent events.SQSEvent) error {
	// var first map[string]TranslateTextResponse
	// first = make(map[string]TranslateTextResponse)

	keyMapList := make([]TranslateTextResponse, 0)

	for _, val := range snsEvent.Records {
		fmt.Fprintln(os.Stdout, []any{"received queue messages .. %s", val.Body}...)
		fmt.Fprintln(os.Stdout, []any{"received queue attr .. %s", val.Attributes}...)

		result := make(map[string]TranslateTextResponse)

		json.Unmarshal([]byte(*&val.Body), &result)

		originalText := result["text"].OriginalText
		translatedText := result["text"].TranslatedText
		sourceType := result["text"].SourceType

		record, keyMap, allKeys, languageCol, rowLength, err := readFile(client, sourceType)
		if err != nil {
			fmt.Println(err)
			// return err
			continue
		}

		//if the key is not contains in the Map
		_, ok := keyMap[originalText]

		if !ok {
			//key is not in the csv
			keyMapList = append(keyMapList, TranslateTextResponse{OriginalText: originalText, TranslatedText: translatedText, SourceType: sourceType})
		}

		writeErr := writeToCsv(client.s3Client, keyMapList, record, allKeys, languageCol, rowLength)
		if writeErr == nil {
			//return writeErr
			fmt.Println(writeErr)
			continue
		}
	}
	fmt.Fprintln(os.Stdout, []any{"write queue messages ...%s", len(keyMapList)}...)
	return nil
}

func (client *Clients) HttpHandler(w http.ResponseWriter, r *http.Request) {
	// var first map[string]TranslateTextResponse
	// first = make(map[string]TranslateTextResponse)

	var keyMapList []TranslateTextResponse
	var body map[string][]map[string]TranslateTextResponse

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		panic(err)
	}

	for _, val := range body["data"] {
		fmt.Fprintln(os.Stdout, []any{"Received msg from queue, msg is %s", val["text"]}...)
		// msg, err := client.sqsClient.ReceiveMessage(ctx, &client.messageInput)
		// if err != nil {
		// 	panic(err)
		// }

		// json.Unmarshal([]byte(val.SNS.Message), &first)
		// languageTo := first["text"].SourceType

		originalText := val["text"].OriginalText
		translatedText := val["text"].TranslatedText
		sourceType := val["text"].SourceType

		record, keyMap, allKeys, languageCol, rowLength, err := readFile(client, sourceType)
		if err != nil {
			panic(err)
		}

		//send batch result
		// for _, v := range msg.Messages {

		//if the key is not contains in the Map
		_, ok := keyMap[originalText]

		if !ok {
			//key is not in the csv
			keyMapList = append(keyMapList, TranslateTextResponse{OriginalText: originalText, TranslatedText: translatedText, SourceType: sourceType})
		}
		// }
		writeToCsv(client.s3Client, keyMapList, record, allKeys, languageCol, rowLength)
	}
	fmt.Fprintln(os.Stdout, []any{"write to csv queue messages ...%s", keyMapList}...)

}

// read
func readFile(client *Clients, languageTo string) ([][]string, map[string]string, map[string]int, int, int, error) {
	buc := "translate-s3-proitzen"
	key := "input/aws localeasy.csv"

	object := s3.GetObjectInput{
		Bucket: &buc,
		Key:    &key,
	}

	fileOut, err := client.s3Client.GetObject(context.TODO(), &object)
	if err != nil {
		panic(err)
	}

	defer fileOut.Body.Close()
	return csvToJson(fileOut.Body, languageTo)
}

func csvToJson(body io.ReadCloser, languageTo string) ([][]string, map[string]string, map[string]int, int, int, error) {
	fmt.Println("csvToJson starting..........")
	var languageCol int
	reader := csv.NewReader(body)
	var keyMap map[string]string
	var allKeys map[string]int
	var rowLength int

	record, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, -1, -1, err
	}

	var key string
	var val string
	keyMap = make(map[string]string)
	allKeys = make(map[string]int)

	for i, row := range record {
		rowLength = len(row)
		//get the csv headers
		if i == 0 {
			for colIndex, v := range row {
				if languageTo == v {
					fmt.Fprintln(os.Stdout, []any{"languahe code is %s index is %s", v, colIndex}...)
					languageCol = colIndex
				}
			}
		} else {
			//csv body values
			for col, v := range row {
				if col == 0 {
					key = v
					allKeys[v] = i
				}
				if col == languageCol && v != "" {
					val = v
					keyMap[key] = val
				}
			}
		}
	}
	return record, keyMap, allKeys, languageCol, rowLength, nil
}

// write
func writeToCsv(s3Client *s3.Client, cacheMissItems []TranslateTextResponse, records [][]string, allkeysWithColumnId map[string]int, languageCol int, rowLength int) error {
	bucket := "translate-s3-proitzen"
	key := "input/aws localeasy.csv"
	var buffer bytes.Buffer
	var newRecord []string

	fmt.Fprintln(os.Stdout, []any{"cache miss item..%s", cacheMissItems}...)
	fmt.Fprintln(os.Stdout, []any{"all records item in s3 buckey..%s", records}...)

	if len(cacheMissItems) == 0 {
		return nil
	}

	for _, v := range cacheMissItems {
		val, ok := allkeysWithColumnId[v.OriginalText]
		if ok {
			//key is already in the csv file
			records[val][languageCol] = v.TranslatedText
		} else {
			newRecord = make([]string, rowLength)
			//create a new record on the csv file
			newRecord[0] = v.OriginalText
			newRecord[languageCol] = v.TranslatedText
			records = append(records, newRecord)
		}
	}

	writer := csv.NewWriter(&buffer)

	err := writer.WriteAll(records)
	if err != nil {
		panic(err)
	}

	s3Out := s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(buffer.String()),
		ContentType: aws.String("text/csv"),
	}

	_, s3Err := s3Client.PutObject(context.TODO(), &s3Out)
	if s3Err != nil {
		panic(s3Err)
	}

	return nil
}

func main() {
	acessKey := os.Getenv("ACCESS_KEY")
	secret := os.Getenv("SECRET")
	region := os.Getenv("AWS_REGION_NAME")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(acessKey, secret, "")),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	sqsClient := sqs.NewFromConfig(cfg)

	client, err := NewClient(sqsClient, s3Client)

	lambda.Start(client.Handler)
	// lambda.Start(client.Handler)
	/* http.HandleFunc("/queue", client.HttpHandler)
	http.ListenAndServe(":8080", nil)
	return */
}
