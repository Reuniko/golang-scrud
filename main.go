package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// Base CRUD struct for MySQL operations
type CRUD struct {
	DB        *sql.DB
	Table     string
	Structure map[string]map[string]string // Structure with TYPE, NAME, NOT_NULL, DEFAULT, INDEX, UNIQUE, AUTO_INCREMENT
}

// NewCRUD initializes a CRUD instance for a specific table with structure
func NewCRUD(db *sql.DB, table string, structure map[string]map[string]string) *CRUD {
	return &CRUD{
		DB:        db,
		Table:     table,
		Structure: structure,
	}
}

// Create inserts a new record into the database with safety checks
func (c *CRUD) Create(data map[string]any) error {
	// Check for NOT_NULL fields without default value or AUTO_INCREMENT
	for col, properties := range c.Structure {
		if properties["NOT_NULL"] == "true" && properties["DEFAULT"] == "" && properties["AUTO_INCREMENT"] != "true" {
			if _, ok := data[col]; !ok {
				return fmt.Errorf("field '%s' cannot be null", col)
			}
		}
	}

	// Prepare the query for insertion
	columns := []string{}
	values := []any{}
	placeholders := []string{}

	for col := range c.Structure {
		columns = append(columns, col)
		values = append(values, data[col]) // Assumes provided data map has correct keys
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		c.Table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))
	_, err := c.DB.Exec(query, values...)
	return err
}

// Update modifies an existing record with safety checks
func (c *CRUD) Update(id string, data map[string]any) error {
	// Check for NOT_NULL fields without default value or AUTO_INCREMENT
	for col, properties := range c.Structure {
		if properties["NOT_NULL"] == "true" && properties["DEFAULT"] == "" && properties["AUTO_INCREMENT"] != "true" {
			if _, ok := data[col]; !ok {
				return fmt.Errorf("field '%s' cannot be null", col)
			}
		}
	}

	// Prepare the query for updating
	setClauses := []string{}
	values := []any{}

	for col := range data {
		if value, ok := data[col]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))
			values = append(values, value)
		}
	}

	// Assuming the primary key column name is "id"
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", c.Table, strings.Join(setClauses, ", "))
	values = append(values, id)

	_, err := c.DB.Exec(query, values...)
	return err
}

// Delete removes a record from the database
func (c *CRUD) Delete(id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", c.Table)
	_, err := c.DB.Exec(query, id)
	return err
}

// PrepareWhere constructs a WHERE clause for SQL from a filter map.
func (c *CRUD) PrepareWhere(filter map[string]any) (string, []error) {
	return c.PrepareWhereWithRootLogic(filter, "AND")
}

func (c *CRUD) PrepareWhereWithRootLogic(filter map[string]any, logic string) (string, []error) {
	var conditions []string
	var errors []error

	for key, value := range filter {
		// Check for OR grouping
		if key == "[OR]" || strings.HasPrefix(key, "[OR]") {
			orGroup := value.(map[string]any)
			orClause, orErrors := c.PrepareWhereWithRootLogic(orGroup, "OR")
			if len(orErrors) > 0 {
				errors = append(errors, orErrors...)
			}
			if orClause != "" {
				conditions = append(conditions, fmt.Sprintf("(%s)", orClause))
			}
		} else if key == "[AND]" || strings.HasPrefix(key, "[AND]") {
			// Handle AND group in a recursive manner
			andGroup := value.(map[string]any)
			andClause, andErrors := c.PrepareWhereWithRootLogic(andGroup, "AND")
			if len(andErrors) > 0 {
				errors = append(errors, andErrors...)
			}
			if andClause != "" {
				conditions = append(conditions, fmt.Sprintf("(%s)", andClause))
			}
		} else {
			// Determine the base column name and the operator
			var column string
			var comparison string

			if strings.HasSuffix(key, ">") {
				column = strings.TrimSuffix(key, ">")
				comparison = ">"
			} else if strings.HasSuffix(key, "<") {
				column = strings.TrimSuffix(key, "<")
				comparison = "<"
			} else if strings.HasSuffix(key, "=") {
				column = strings.TrimSuffix(key, "=")
				comparison = "="
			} else if strings.HasSuffix(key, "%") {
				column = strings.TrimSuffix(key, "%")
				comparison = "LIKE"
				// Enclose value in wildcards for LIKE operator
				value = "%" + value.(string) + "%"
			} else {
				column = key
				comparison = "="
			}

			// Safety check: Ensure column exists in the Structure
			if _, ok := c.Structure[column]; !ok {
				errors = append(errors, fmt.Errorf("field '%s' does not exist in the structure", column))
				continue // Skip building condition for this field
			}

			// Build the condition
			conditions = append(conditions, fmt.Sprintf("%s %s \"%v\"", column, comparison, value))
		}
	}

	// Join conditions with AND
	if len(conditions) > 0 {
		return strings.Join(conditions, " "+logic+" "), nil
	}
	return "", errors // Return empty string with gathered errors
}

// // PrepareWhere helper function for processing map with specific operator
// func PrepareWhere(filter map[string]any, operator string) string {
// 	// Modify the original context: Adjust logic for AND/OR specific conditions
// 	// This wrapper allows flexibility in operators for higher-level processing
// 	// Implementation will vary based on how you want to structure your logic
// }

// Synchronize checks if the table exists and matches the structure; if not, it creates or alters it.
func (c *CRUD) Synchronize() error {
	// Check if the table exists
	var exists int
	query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", c.Table)
	err := c.DB.QueryRow(query).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %s; query: %s", err, query)
	}

	if exists == 0 {
		// Table does not exist, create it
		fieldDefinitions := []string{}
		var primaryKeyColumn string
		for col, properties := range c.Structure {
			fieldDef := fmt.Sprintf("%s %s", col, properties["TYPE"])
			if properties["NOT_NULL"] == "true" {
				fieldDef += " NOT NULL"
			}
			if properties["DEFAULT"] != "" {
				fieldDef += fmt.Sprintf(" DEFAULT %s", properties["DEFAULT"])
			}
			if properties["AUTO_INCREMENT"] == "true" {
				fieldDef += " AUTO_INCREMENT"
				primaryKeyColumn = col // Set the primary key column if it's auto-increment
			}
			fieldDefinitions = append(fieldDefinitions, fieldDef)
		}

		// Create the table with the primary key
		createTableQuery := fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))", c.Table,
			strings.Join(fieldDefinitions, ", "), primaryKeyColumn)
		_, err := c.DB.Exec(createTableQuery)
		if err != nil {
			return fmt.Errorf("failed to create table: %s; query: %s", err, createTableQuery)
		}
	} else {
		// Table exists, check and update structure
		rows, err := c.DB.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", c.Table))
		if err != nil {
			return fmt.Errorf("failed to show columns: %s; query: SHOW COLUMNS FROM %s", err, c.Table)
		}
		defer rows.Close()

		currentColumns := make(map[string]map[string]string)
		for rows.Next() {
			var field, colType, isNull, key, defaultValue sql.NullString
			var extra string
			err := rows.Scan(&field, &colType, &isNull, &key, &defaultValue, &extra)
			if err != nil {
				return fmt.Errorf("failed to scan column: %s; query: SHOW COLUMNS FROM %s", err, c.Table)
			}

			currentColumns[field.String] = map[string]string{
				"COL_TYPE": colType.String,      // Convert from sql.NullString to string
				"NULL":     isNull.String,       // Convert to string
				"KEY":      key.String,          // Convert to string
				"DEFAULT":  defaultValue.String, // Handle default as string allowing NULL
				"EXTRA":    extra,               // extra is a standard string
			}
		}

		// Alter the table if fields differ from the structure
		for col, properties := range c.Structure {
			if currentColInfo, ok := currentColumns[col]; ok {
				// Check if type matches (simplified comparison, can be expanded)
				if currentColInfo["COL_TYPE"] != properties["TYPE"] ||
					(properties["NOT_NULL"] == "true" && currentColInfo["NULL"] == "YES") ||
					(properties["DEFAULT"] != "" && currentColInfo["DEFAULT"] != properties["DEFAULT"]) {
					alterQuery := fmt.Sprintf("ALTER TABLE %s MODIFY %s %s", c.Table, col, properties["TYPE"])
					if properties["NOT_NULL"] == "true" {
						alterQuery += " NOT NULL"
					}
					if properties["DEFAULT"] != "" {
						alterQuery += fmt.Sprintf(" DEFAULT %s", properties["DEFAULT"])
					}
					if properties["AUTO_INCREMENT"] == "true" {
						alterQuery += " AUTO_INCREMENT"
					}
					_, err := c.DB.Exec(alterQuery)
					if err != nil {
						return fmt.Errorf("failed to alter table structure: %s; query: %s", err, alterQuery)
					}
				}
			} else {
				// Column does not exist, add it
				addQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", c.Table, col, properties["TYPE"])
				if properties["NOT_NULL"] == "true" {
					addQuery += " NOT NULL"
				}
				if properties["DEFAULT"] != "" {
					addQuery += fmt.Sprintf(" DEFAULT %s", properties["DEFAULT"])
				}
				if properties["AUTO_INCREMENT"] == "true" {
					addQuery += " AUTO_INCREMENT"
				}
				_, err := c.DB.Exec(addQuery)
				if err != nil {
					return fmt.Errorf("failed to add new column: %s; query: %s", err, addQuery)
				}
			}
		}

		// Ensure the AUTO_INCREMENT column is set as a primary key only if it is not already
		for col, properties := range c.Structure {
			if properties["AUTO_INCREMENT"] == "true" {
				// Check if it is already a primary key
				if currentColInfo, ok := currentColumns[col]; ok && currentColInfo["KEY"] != "PRI" {
					primaryKeyQuery := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", c.Table, col)
					_, err := c.DB.Exec(primaryKeyQuery)
					if err != nil {
						return fmt.Errorf("failed to set primary key: %s; query: %s", err, primaryKeyQuery)
					}
				}
				break
			}
		}
	}

	return nil
}

// UserCRUD struct that embeds CRUD for user-specific operations
type UserCRUD struct {
	*CRUD
}

// NewUserCRUD initializes an instance of UserCRUD with the user structure
func NewUserCRUD(db *sql.DB) *UserCRUD {
	// Define user structure with additional keys: DEFAULT, INDEX, UNIQUE, AUTO_INCREMENT
	userStructure := map[string]map[string]string{
		"id": {
			"TYPE":           "int",
			"NAME":           "ID",
			"NOT_NULL":       "true",
			"DEFAULT":        "",     // Default value for ID
			"INDEX":          "yes",  // Index for ID
			"UNIQUE":         "yes",  // ID should be unique
			"AUTO_INCREMENT": "true", // ID is auto-incrementing
		},
		"name": {
			"TYPE":           "varchar(255)",
			"NAME":           "Name",
			"NOT_NULL":       "true",
			"DEFAULT":        "",      // Default value for Name
			"INDEX":          "yes",   // Index for Name
			"UNIQUE":         "no",    // Name is not unique
			"AUTO_INCREMENT": "false", // Name is not auto-incrementing
		},
		"email": {
			"TYPE":           "varchar(255)",
			"NAME":           "Email",
			"NOT_NULL":       "true",
			"DEFAULT":        "",      // Default value for Email
			"INDEX":          "no",    // No index on Email
			"UNIQUE":         "yes",   // Email should be unique
			"AUTO_INCREMENT": "false", // Email is not auto-incrementing
		},
	}

	// Passing the user structure to the CRUD constructor
	crud := NewCRUD(db, "users", userStructure)
	return &UserCRUD{
		CRUD: crud,
	}
}

// Override Delete method to forbid deleting user with id = 1
func (u *UserCRUD) Delete(id string) error {
	if id == "1" {
		return fmt.Errorf("deletion forbidden for user with ID = 1")
	}
	return u.CRUD.Delete(id) // Call the original Delete method from CRUD
}

// Example Usage
func main() {
	// Setup database connection
	dsn := "root:@tcp(localhost:3333)/golang" // Updated DSN
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a UserCRUD instance for the 'users' table
	userCRUD := NewUserCRUD(db)

	err = userCRUD.Synchronize()
	if err != nil {
		log.Fatal(err)
	}

	// Example of creating a user directly using the Create method from CRUD
	err = userCRUD.Create(map[string]any{
		"name":  "Alice Smith",
		"email": "alice.smith@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Attempt to create a user without required fields
	err = userCRUD.Create(map[string]any{
		"email": "no_name@example.com", // Missing the 'name' field
	})
	if err != nil {
		fmt.Println("Error:", err) // Should return an error about name being nil
	}

	// Attempt to update user with ID 1
	err = userCRUD.Update("1", map[string]any{
		"name":  "Updated Name",
		"email": "updated.email@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Attempt to update user with missing required fields
	err = userCRUD.Update("2", map[string]any{})
	if err != nil {
		fmt.Println("Error:", err) // Should return an error about required fields
	}

	// Attempt to delete user with ID 1
	err = userCRUD.Delete("1")
	if err != nil {
		fmt.Println("Error:", err) // Should state that deletion is forbidden for ID 1
	}

	// Example of deleting another user (assuming user ID 2 exists)
	err = userCRUD.Create(map[string]any{
		"name":  "Bob Johnson",
		"email": "bob.johnson@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}
	err = userCRUD.Delete("2") // Attempt to delete user with ID 2
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("User with ID 2 deleted successfully.")
	}

	whereClause, errList := userCRUD.PrepareWhere(map[string]any{
		"name":   "Bob Johnson",
		"email%": "@example.com",
		"[OR]": map[string]any{
			"id>": 1,
			"id<": 2,
			"[AND]": map[string]any{
				"id>": 3,
				"id<": 4,
			},
			"[AND]333": map[string]any{
				"id>": 5,
				"id<": 6,
			},
		},
		"[OR]222": map[string]any{
			"id>": 7,
			"id<": 8,
		},
	})
	if len(errList) > 0 {
		for _, err := range errList {
			fmt.Println("Error:", err) // Outputs errors for each invalid field
		}
	} else {
		fmt.Println(whereClause) // Outputs the WHERE clause
	}
}
