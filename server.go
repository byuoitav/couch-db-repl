package main

import (
	"net/http"

	"github.com/byuoitav/authmiddleware"
	"github.com/byuoitav/common"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/couch-db-repl/handlers"
	"github.com/byuoitav/couch-db-repl/replication"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

func main() {
	port := ":7012"
	router := common.NewRouter()

	router.Pre(middleware.RemoveTrailingSlash())
	router.Use(middleware.CORS())

	// Use the `secure` routing group to require authentication
	secure := router.Group("", echo.WrapMiddleware(authmiddleware.Authenticate))

	secure.PUT("/log-level/:level", log.SetLogLevel)
	secure.GET("/log-level", log.GetLogLevel)

	secure.GET("/replication/start", handlers.ReplicateNow)

	server := http.Server{
		Addr:           port,
		MaxHeaderBytes: 1024 * 10,
	}

	go router.StartServer(&server)

	replication.Init()
	if err := replication.Start(); err != nil {
		log.L.Fatal(err)
	}
}
