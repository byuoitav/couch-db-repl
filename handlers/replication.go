package handlers

import (
	"net/http"

	"github.com/byuoitav/couch-db-repl/replication"
	"github.com/labstack/echo"
)

func ReplicateNow(context echo.Context) error {
	err := replication.ReplicateNow()
	if err != nil {
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	return context.JSON(http.StatusOK, "replication scheduled")

}
