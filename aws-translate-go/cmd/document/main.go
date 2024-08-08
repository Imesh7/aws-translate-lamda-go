package main


import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/translate"
	"github.com/google/uuid"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	//	"github.com/aws/aws-lambda-go/lambda"
)

type TranslateTextResponse struct {
	SourceType     string `json:"sourceType" bson:"sourceType"`
	OriginalText   string `json:"originalText" bson:"originalText"`
	TranslatedText string `json:"translatedText" bson:"translatedText"`
}
type TranslateTextRequest struct {
	From     string   `json:"from" bson:"from"`
	To       string   `json:"to" bson:"to"`
	TextList []string `json:"text" bson:"text"`
}

func Handler(ctx context.Context, request events.LambdaFunctionURLRequest) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{Body: "It works!", StatusCode: 200}, nil
}



func main() {

	acessKey := os.Getenv("ACCESS_KEY")
	secret := os.Getenv("SECRET")
	region := os.Getenv("AWS_REGION_NAME")

	//initialize AWS Translation config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(acessKey, secret, "")),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(cfg)
	client := translate.NewFromConfig(cfg)

	sqsClient := sqs.NewFromConfig(cfg)

	trHandler := NewHandler(client, s3Client, sqsClient)

	lambda.Start(trHandler.translate)

	// http.HandleFunc("/translate", trHandler.translate)
	// http.ListenAndServe(":8080", nil)
	// return
}

/* func main() {
	fmt.Println("app started")

	acessKey := "AKIA6ODUZU32UGEYV257"
	secret := "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"

	//initialize AWS Translation config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(acessKey, secret, "")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(cfg)
	client := translate.NewFromConfig(cfg)

	sqsClient := sqs.NewFromConfig(cfg)
	re := &sqs.SendMessageBatchInput{
		QueueUrl: &queueUrl,
	}

	trHandler := NewHandler(client, s3Client, sqsClient, re)

	http.HandleFunc("/translate", trHandler.translate)
	http.ListenAndServe(":8080", nil)
	return
} */

type translateHandler struct {
	client       *translate.Client
	s3Client     *s3.Client
	sqsClient    *sqs.Client
	messageInput *sqs.SendMessageBatchInput
}

func NewHandler(client *translate.Client, s3Client *s3.Client, sqsClient *sqs.Client) *translateHandler {
	return &translateHandler{
		client:    client,
		s3Client:  s3Client,
		sqsClient: sqsClient,
	}
}

/* func (tr *translateHandler) translate(w http.ResponseWriter, r *http.Request) {
	var body TranslateTextRequest
	wg := sync.WaitGroup{}
	var cacheMissItems []TranslateTextResponse
	//var record [][]string

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		panic(err)
	}

	from := body.From
	to := body.To
	textList := body.TextList

	ch := make(chan TranslateTextResponse, len(textList))
	translatedTextList := make([]TranslateTextResponse, len(textList))
	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	_, cacheMap, _, _, _, err := readFile(tr, to)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(textList); i++ {
		wg.Add(1)
		go func(i int) {
			textInput := translate.TranslateTextInput{
				SourceLanguageCode: &from,
				TargetLanguageCode: &to,
				Text:               &textList[i],
			}
			var res TranslateTextResponse

			val, ok := cacheMap[*textInput.Text]
			// If the key exists
			if ok {
				fmt.Fprintln(os.Stdout, []any{"Cache Hit %s for %s ", *textInput.Text, val}...)
				fmt.Println(time.Now())

				res.OriginalText = *textInput.Text
				res.SourceType = to
				res.TranslatedText = val

			} else {
				fmt.Println("Cache Miss.................")

				out, err := tr.client.TranslateText(context.TODO(), &textInput)
				if err != nil {
					panic(err)
				}

				//fmt.Println(*out.TranslatedText)
				fmt.Println(time.Now())

				res.OriginalText = *textInput.Text
				res.SourceType = *out.TargetLanguageCode
				res.TranslatedText = *out.TranslatedText

				cacheMissItems = append(cacheMissItems, res)
			}
			ch <- res
			wg.Done()
		}(i)
	}

	wg.Wait()
	close(ch)
	//this should be add to queue
	go tr.AddToQueueAsBatch(cacheMissItems)
	//swriteToCsv(tr, cacheMissItems, record, allKeys, languageCol, rowLength)

	for i := 0; i < len(textList); i++ {
		data := <-ch
		translatedTextList[i] = data
		fmt.Fprintln(os.Stdout, []any{"channel added item %s", data}...)
	}

	//fmt.Println(translatedTextList)
	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(translatedTextList)
	return
} */

func (tr *translateHandler) translate(ctx context.Context, request events.LambdaFunctionURLRequest) (events.APIGatewayProxyResponse, error) {
	var body TranslateTextRequest
	wg := sync.WaitGroup{}
	var cacheMissItems []TranslateTextResponse
	//var record [][]string
	uniqueWordMap := make(map[string]bool, 0)

	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		fmt.Println(err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Json unmarshal error",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}, nil
	}

	from := body.From
	to := body.To
	textList := body.TextList

	//check duplicates
	for _, v := range textList {
		uniqueWordMap[v] = true
	}

	uniqueTextList := make([]string, 0)

	for key, _ := range uniqueWordMap {
		uniqueTextList = append(uniqueTextList, key)
	}

	ch := make(chan TranslateTextResponse, len(uniqueTextList))
	translatedTextList := make([]TranslateTextResponse, len(uniqueTextList))
	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	_, cacheMap, _, _, _, err := readFile(tr, to)
	if err != nil {
		fmt.Println(err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Json encode error",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}, nil
	}

	for i := 0; i < len(uniqueTextList); i++ {
		wg.Add(1)
		go func(i int) {
			textInput := translate.TranslateTextInput{
				SourceLanguageCode: &from,
				TargetLanguageCode: &to,
				Text:               &uniqueTextList[i],
			}
			var res TranslateTextResponse

			val, ok := cacheMap[*textInput.Text]
			// If the key exists
			if ok {
				fmt.Fprintln(os.Stdout, []any{"Cache Hit %s for %s ", *textInput.Text, val}...)
				fmt.Println(time.Now())

				res.OriginalText = *textInput.Text
				res.SourceType = to
				res.TranslatedText = val

			} else {
				fmt.Println("Cache Miss.................")

				out, err := tr.client.TranslateText(context.TODO(), &textInput)
				if err != nil {
					panic(err)
				}
				fmt.Println(time.Now())

				res.OriginalText = *textInput.Text
				res.SourceType = *out.TargetLanguageCode
				res.TranslatedText = *out.TranslatedText

				cacheMissItems = append(cacheMissItems, res)
			}
			ch <- res
			wg.Done()
		}(i)
	}

	wg.Wait()
	close(ch)
	//this should be add to queue
	// var items []types.SendMessageBatchRequestEntry
	// items = make([]types.SendMessageBatchRequestEntry, len(cacheMissItems))
	// for _, v := range cacheMissItems {
	// 	items = append(items, types.SendMessageBatchRequestEntry{Id: aws.String(uuid.NewString()), MessageBody: aws.String(string(v.OriginalText))})
	// }
	// sendBatchMessages(tr.sqsClient, queueUrl, items)
	tr.AddToQueueAsBatch(cacheMissItems)
	//writeToCsv(tr, cacheMissItems, record, allKeys, languageCol, rowLength)

	for i := 0; i < len(uniqueTextList); i++ {
		data := <-ch
		translatedTextList[i] = data
		fmt.Fprintln(os.Stdout, []any{"channel added item %s", data}...)
	}

	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	res, jErr := json.Marshal(translatedTextList)
	if jErr != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Json marshal error",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}, nil
	}
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(res),
		Headers: map[string]string{
			"Content-Type":                     "application/json",
			"Access-Control-Allow-Origin":      "*",
			"Access-Control-Allow-Headers":     "Content-Type",
			"Access-Control-Allow-Methods":     "GET, POST",
			"Access-Control-Allow-Credentials": "true",
		},
	}, nil
}

func readFile(tr *translateHandler, languageTo string) ([][]string, map[string]string, map[string]int, int, int, error) {
	buc := os.Getenv("BUCKET")
	key := os.Getenv("FILE_LOCATION")

	object := s3.GetObjectInput{
		Bucket: &buc,
		Key:    &key,
	}

	fileOut, err := tr.s3Client.GetObject(context.TODO(), &object)
	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, -1, -1, err
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

func writeToCsv(tr *translateHandler, cacheMissItems []TranslateTextResponse, records [][]string, allkeysWithColumnId map[string]int, languageCol int, rowLength int) {
	bucket := os.Getenv("BUCKET")
	key := os.Getenv("FILE_LOCATION")
	var buffer bytes.Buffer
	var newRecord []string

	// if len(cacheMissItems) == 0 {
	// 	return
	// }

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

	_, s3Err := tr.s3Client.PutObject(context.TODO(), &s3Out)
	if s3Err != nil {
		panic(s3Err)
	}

}

func (tr *translateHandler) AddToQueueAsBatch(cacheMissItems []TranslateTextResponse) {
	entries := make([]types.SendMessageBatchRequestEntry, len(cacheMissItems))

	if len(cacheMissItems) == 0 {
		return
	}

	for i, v := range cacheMissItems {
		message := map[string]map[string]string{
			"text": {
				"OriginalText":   v.OriginalText,
				"TranslatedText": v.TranslatedText,
				"SourceType":     v.SourceType,
			},
		}

		messageBody, err := json.Marshal(message)
		if err != nil {
			fmt.Println("failed to marshal message: %w", err)
		}
		entry := types.SendMessageBatchRequestEntry{Id: aws.String(uuid.New().String()), MessageBody: aws.String(string(messageBody))}
		entries[i] = entry
	}
	queueUrl := os.Getenv("QUEUE_URL")

	matrix := splitInto10Chunks(entries, 10)

	for _, v := range matrix {
		output, err := tr.sqsClient.SendMessageBatch(context.Background(),
			&sqs.SendMessageBatchInput{
				Entries:  v,
				QueueUrl: &queueUrl,
			})

		if err != nil {
			panic(err)
		}

		for _, successful := range output.Successful {
			fmt.Printf("Message ID %s sent successfully\n", *successful.MessageId)
		}

		for _, failed := range output.Failed {
			fmt.Printf("Message ID %s failed to send: %s\n", *failed.Id, *failed.Message)
		}
	}
}

func splitInto10Chunks(cacheMissEntries []types.SendMessageBatchRequestEntry, chunksLength int) [][]types.SendMessageBatchRequestEntry {
	var chunks [][]types.SendMessageBatchRequestEntry
	for chunksLength < len(cacheMissEntries) {
		chunks = append(chunks, cacheMissEntries[0:chunksLength])
		cacheMissEntries = cacheMissEntries[chunksLength:]
	}

	return append(chunks, cacheMissEntries)
}
