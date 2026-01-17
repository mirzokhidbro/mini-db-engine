package api

import (
	"net/http"
	"rdbms/api/handlers"

	"github.com/gin-gonic/gin"
)

func SetUpRouter(h handlers.Handler) (r *gin.Engine) {
	r = gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.Use(customCORSMiddleware())

	baseRouter := r.Group("/api/v1")

	{
		table := baseRouter.Group("tables")
		table.Use().POST("create-table", h.CreateTable)
	}

	{
		table := baseRouter.Group("records")
		table.Use().POST("insert", h.InsertRecord)
		table.Use().POST("query", h.GetAllRecords)
		table.Use().POST("delete", h.DeleteRecords)
		table.Use().POST("update", h.UpdateRecords)
	}
	return
}

func customCORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, PATCH, DELETE")

		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, Accept, Origin, Cache-Control, X-Requested-With")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
