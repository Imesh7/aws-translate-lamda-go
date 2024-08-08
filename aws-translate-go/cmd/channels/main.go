package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/translate"
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

func main() {
	acessKey := "xxx"
	secret := "xxx"

	//initialize AWS Translation config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(acessKey, secret, "")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("app started")
	client := translate.NewFromConfig(cfg)

	trHanlder := NewHandler(client)

	http.HandleFunc("/translate", trHanlder.translate)
	http.ListenAndServe(":8080", nil)
}

type translateHandler struct {
	client *translate.Client
}

func NewHandler(client *translate.Client) *translateHandler {
	return &translateHandler{
		client: client,
	}
}

func (tr *translateHandler) translate(w http.ResponseWriter, r *http.Request) {
	var body TranslateTextRequest

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		panic(err)
	}

	from := body.From
	to := body.To
	textList := body.TextList

	ch := make(chan TranslateTextResponse)
	translatedTextList := make([]TranslateTextResponse,0)
	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	go func() {
		for i := 0; i < len(textList); i++ {
			textInput := translate.TranslateTextInput{
				SourceLanguageCode: &from,
				TargetLanguageCode: &to,
				Text:               &textList[i],
			}
			var res TranslateTextResponse
			out, err := tr.client.TranslateText(context.TODO(), &textInput)
			if err != nil {
				panic(err)
			}
			fmt.Println(*out.TranslatedText)
			fmt.Println(time.Now())

			res.OriginalText = *textInput.Text
			res.SourceType = *out.TargetLanguageCode
			res.TranslatedText = *out.TranslatedText

			ch <- res
		}
		defer close(ch)
	}()

	for d := range ch {
		translatedTextList = append(translatedTextList, d)
	}

	fmt.Println(translatedTextList)
	fmt.Fprintln(os.Stdout, []any{"Length is -- %s ", len(translatedTextList)}...)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(translatedTextList)

}
