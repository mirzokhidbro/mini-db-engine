package models

type CreateTableRequest struct {
	Name    string         `json:"name"`
	Columns []CreateColumn `json:"columns"`
}

type CreateColumn struct {
	Name      string `json:"name" binding:"required"`
	Type      int    `json:"type" binding:"required"`
	Length    *int   `json:"length,omitempty"`
	IsIndexed bool   `json:"is_indexed" binding:"required"`
}
