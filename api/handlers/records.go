package handlers

import (
	"encoding/json"
	"rdbms/api/http"
	"rdbms/api/models"
	"rdbms/src/storage"
	"rdbms/utils"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *Handler) InsertRecord(c *gin.Context) {
	var req models.InsertRecordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	items := make([]storage.Item, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		v, ok := req.Values[col.Name]
		if !ok {
			h.handleResponse(c, http.InvalidArgument, "missing column: "+col.Name)
			return
		}

		switch col.Type {
		case storage.TypeInt:
			n, ok := v.(float64)
			if !ok || n != float64(int64(n)) {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be integer")
				return
			}
			items = append(items, storage.Item{Literal: int64(n)})
		case storage.TypeVarchar:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be string")
				return
			}
			if len(s) > col.Length {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" exceeds length")
				return
			}
			items = append(items, storage.Item{Literal: s})
		case storage.TypeFloat:
			f, ok := v.(float64)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be number")
				return
			}
			items = append(items, storage.Item{Literal: f})
		case storage.TypeJSON:
			b, err := json.Marshal(v)
			if err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid json for "+col.Name)
				return
			}
			items = append(items, storage.Item{Literal: string(b)})
		case storage.TypeDate:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be date string YYYY-MM-DD")
				return
			}
			if _, err := time.Parse("2006-01-02", s); err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid date format for "+col.Name)
				return
			}
			items = append(items, storage.Item{Literal: s})
		case storage.TypeTimestamp:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+col.Name+" must be RFC3339 timestamp string")
				return
			}
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid timestamp format for "+col.Name)
				return
			}
			items = append(items, storage.Item{Literal: s})
		}
	}

	if err := h.Stg.Table().Insert(req.Name, storage.Record{Items: items}); err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	h.handleResponse(c, http.Created, "Record inserted")
}

func (h *Handler) GetAllRecords(c *gin.Context) {
	var req models.GetAllRecordsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	filters := make([]storage.Filter, 0, len(req.Filter))
	for _, f := range req.Filter {
		filters = append(filters, storage.Filter{
			Column:   f.Column,
			Operator: f.Operator,
			Value:    f.Value,
		})
	}

	filters, err = utils.SetFilterColumnIndexes(schema, filters)
	if err != nil {
		h.handleResponse(c, http.InvalidArgument, err.Error())
		return
	}
	selectedColumns := storage.SelectedColumns{Columns: req.Columns}
	data, err := h.Stg.Table().GetAllData(req.Name, filters, selectedColumns)
	if err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	h.handleResponse(c, http.OK, data)
}

func (h *Handler) DeleteRecords(c *gin.Context) {
	var req models.DeleteRecordsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	filters := make([]storage.Filter, 0, len(req.Filter))
	for _, f := range req.Filter {
		filters = append(filters, storage.Filter{
			Column:   f.Column,
			Operator: f.Operator,
			Value:    f.Value,
		})
	}

	filters, err = utils.SetFilterColumnIndexes(schema, filters)
	if err != nil {
		h.handleResponse(c, http.InvalidArgument, err.Error())
		return
	}

	deletedCount, err := h.Stg.Table().Delete(req.Name, filters)
	if err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	h.handleResponse(c, http.OK, gin.H{
		"deleted_count": deletedCount,
		"message":       "Records deleted successfully",
	})
}

func (h *Handler) UpdateRecords(c *gin.Context) {
	var req models.UpdateRecordsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.handleResponse(c, http.BadRequest, err.Error())
		return
	}

	schema, err := h.Stg.Table().GetTableSchema(req.Name + ".schema")
	if err != nil {
		h.handleResponse(c, http.NOT_FOUND, err.Error())
		return
	}

	if len(req.Values) == 0 {
		h.handleResponse(c, http.InvalidArgument, "values cannot be empty")
		return
	}

	updatedFields := make(map[string]any)
	for colName, v := range req.Values {
		var col *storage.Column
		for i := range schema.Columns {
			if schema.Columns[i].Name == colName {
				col = &schema.Columns[i]
				break
			}
		}
		if col == nil {
			h.handleResponse(c, http.InvalidArgument, "unknown column: "+colName)
			return
		}

		switch col.Type {
		case storage.TypeInt:
			n, ok := v.(float64)
			if !ok || n != float64(int64(n)) {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" must be integer")
				return
			}
			updatedFields[colName] = int64(n)
		case storage.TypeVarchar:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" must be string")
				return
			}
			if len(s) > col.Length {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" exceeds length")
				return
			}
			updatedFields[colName] = s
		case storage.TypeFloat:
			f, ok := v.(float64)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" must be number")
				return
			}
			updatedFields[colName] = f
		case storage.TypeJSON:
			b, err := json.Marshal(v)
			if err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid json for "+colName)
				return
			}
			updatedFields[colName] = string(b)
		case storage.TypeDate:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" must be date string YYYY-MM-DD")
				return
			}
			if _, err := time.Parse("2006-01-02", s); err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid date format for "+colName)
				return
			}
			updatedFields[colName] = s
		case storage.TypeTimestamp:
			s, ok := v.(string)
			if !ok {
				h.handleResponse(c, http.InvalidArgument, "column "+colName+" must be RFC3339 timestamp string")
				return
			}
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				h.handleResponse(c, http.InvalidArgument, "invalid timestamp format for "+colName)
				return
			}
			updatedFields[colName] = s
		}
	}

	filters := make([]storage.Filter, 0, len(req.Filter))
	for _, f := range req.Filter {
		filters = append(filters, storage.Filter{
			Column:   f.Column,
			Operator: f.Operator,
			Value:    f.Value,
		})
	}

	filters, err = utils.SetFilterColumnIndexes(schema, filters)
	if err != nil {
		h.handleResponse(c, http.InvalidArgument, err.Error())
		return
	}

	updatedCount, err := h.Stg.Table().Update(req.Name, updatedFields, filters)
	if err != nil {
		h.handleResponse(c, http.InternalServerError, err.Error())
		return
	}

	h.handleResponse(c, http.OK, gin.H{
		"updated_count": updatedCount,
		"message":       "Records updated successfully",
	})
}
