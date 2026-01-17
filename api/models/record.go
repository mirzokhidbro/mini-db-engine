package models

type InsertRecordRequest struct {
	Name   string         `json:"name" binding:"required"`
	Values map[string]any `json:"values" binding:"required"`
}

type GetAllRecordsRequest struct {
	Name    string              `json:"name" binding:"required"`
	Filter  []FilterRequestItem `json:"filter"`
	Columns []string            `json:"select"`
}

type FilterRequestItem struct {
	Column   string `json:"column" binding:"required"`
	Operator string `json:"operator" binding:"required"`
	Value    any    `json:"value" binding:"required"`
}

type DeleteRecordsRequest struct {
	Name   string              `json:"name" binding:"required"`
	Filter []FilterRequestItem `json:"filter"`
}

type UpdateRecordsRequest struct {
	Name   string              `json:"name" binding:"required"`
	Values map[string]any      `json:"values" binding:"required"`
	Filter []FilterRequestItem `json:"filter"`
}
