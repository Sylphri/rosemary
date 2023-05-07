#[cfg(test)]
mod tests {
    use crate::*;
    
    // --- parse_table_schema() ---
    #[test]
    fn valid_table_schema() {
        let schema = parse_table_schema("./src/tests_input/valid_table_schema.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
        let schema = schema.unwrap();
        assert!(schema.name == "TestTable");
        assert!(schema.cols.len() == 3);
        assert!(schema.cols[0].name == "id");
        assert!(schema.cols[0].data_type == DataType::Int);
        assert!(schema.cols[1].name == "name");
        assert!(schema.cols[1].data_type == DataType::Str);
        assert!(schema.cols[2].name == "age");
        assert!(schema.cols[2].data_type == DataType::Int);
    }

    #[test]
    #[should_panic(expected = "ERROR: table name can't be empty: ./src/tests_input/schema_with_empty_table_name.tbls")]
    fn schema_with_empty_table_name() {
        let schema = parse_table_schema("./src/tests_input/schema_with_empty_table_name.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    } 

    #[test]
    #[should_panic(expected = "ERROR: column with name 'id' already exists in table scheme: ./src/tests_input/double_column_declaration.tbls")]
    fn double_column_declaration() {
        let schema = parse_table_schema("./src/tests_input/double_column_declaration.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    }

    #[test]
    #[should_panic(expected = "ERROR: unknown column type at line 1 in a file ./src/tests_input/invalid_column_type.tbls")]
    fn invalid_column_type() {
        let schema = parse_table_schema("./src/tests_input/invalid_column_type.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    }
    
    #[test]
    #[should_panic(expected = "ERROR: table name not provided in a file ./src/tests_input/empty_table_schema.tbls")]
    fn empty_table_schema() {
        let schema = parse_table_schema("./src/tests_input/empty_table_schema.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    }
    
    #[test]
    #[should_panic(expected = "ERROR: invalid format for column at line 1 in a file: ./src/tests_input/invalid_column_format.tbls")]
    fn invalid_column_format() {
        let schema = parse_table_schema("./src/tests_input/invalid_column_format.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    }
    
    #[test]
    #[should_panic(expected = "ERROR: empty column name at line 1 in a file ./src/tests_input/empty_column_name.tbls")]
    fn empty_column_name() {
        let schema = parse_table_schema("./src/tests_input/empty_column_name.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
    }

    // --- parse_query() ---
    #[test]
    fn valid_query() {
        let query = "id name select id 10 > filter";
        let expected = vec![
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("id"))},
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("name"))},
            Op::Select,
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("id"))},
            Op::PushWord {data_type: DataType::Int, word_type: WordType::Int(10)},
            Op::More,
            Op::Filter,
        ];
        match parse_query(query) {
            Ok(tokens) => assert!(expected == tokens),
            Err(err)   => assert!(false, "{}", err),
        }
        
        let query = "id 5 != name \"John Watson\" == delete";
        let expected = vec![
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("id"))},
            Op::PushWord {data_type: DataType::Int, word_type: WordType::Int(5)},
            Op::NotEqual,
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("name"))},
            Op::PushWord {data_type: DataType::Str, word_type: WordType::Str(String::from("John Watson"))},
            Op::Equal,
            Op::Delete,
        ];
        match parse_query(query) {
            Ok(tokens) => assert!(expected == tokens),
            Err(err)   => assert!(false, "{}", err),
        }
    }

    #[test]
    #[should_panic(expected = "ERROR: unclosed string literal in a query")]
    fn unclosed_string() {
        let query = "3 \"John Watson 20 insert";
        if let Err(err) = parse_query(query) {
            assert!(false, "{}", err);
        }
    }

    // --- logical_op_check() ---
    #[test]
    fn valid_logical_op() {
        let words = vec![ 
            (DataType::Str, WordType::Str("name".to_string())), 
            (DataType::Str, WordType::Str("John".to_string())),
        ];
        let table = Table {
            schema: TableSchema {
                name: "test".to_string(),
                cols: vec![Col {name: "name".to_string(), data_type: DataType::Str}],
            },
            rows: vec![],
        };
        let expected = Condition {
            idx: 0,
            value: WordType::Str("John".to_string()),
            op: Op::Equal,
        };
        assert!(expected == logical_op_check(Op::Equal, words[0].1.clone(), words[1].clone(), &table).unwrap());
    }

    #[test]
    #[should_panic(expected = "ERROR: invalid argument for `>` operation, expected string but found Int(10)")]
    fn not_string_for_col_name() {
        let words = vec![
            (DataType::Int, WordType::Int(10)), 
            (DataType::Int, WordType::Int(5))
        ];
        let table = Table {
            schema: TableSchema {
                name: "test".to_string(),
                cols: vec![],
            },
            rows: vec![],
        };
        if let Err(err) = logical_op_check(Op::More, words[0].1.clone(), words[1].clone(), &table) {
            assert!(false, "{}", err);
        }
    }
    
    #[test]
    #[should_panic(expected = "ERROR: no such column `age` in table `test`")]
    fn not_existing_column() {
        let words = vec![
            (DataType::Str, WordType::Str("age".to_string())), 
            (DataType::Int, WordType::Int(5))
        ];
        let table = Table {
            schema: TableSchema {
                name: "test".to_string(),
                cols: vec![Col {name: "id".to_string(), data_type: DataType::Int}],
            },
            rows: vec![],
        };
        if let Err(err) = logical_op_check(Op::More, words[0].1.clone(), words[1].clone(), &table) {
            assert!(false, "{}", err);
        }
    }
    
    #[test]
    #[should_panic(expected = "ERROR: invalid argument for `>` operation expected type Int but found type Str(\"8\")")]
    fn types_mismatch_between_col_and_word() {
        let words = vec![
            (DataType::Str, WordType::Str("id".to_string())), 
            (DataType::Str, WordType::Str("8".to_string()))
        ];
        let table = Table {
            schema: TableSchema {
                name: "test".to_string(),
                cols: vec![Col {name: "id".to_string(), data_type: DataType::Int}],
            },
            rows: vec![],
        };
        if let Err(err) = logical_op_check(Op::More, words[0].1.clone(), words[1].clone(), &table) {
            assert!(false, "{}", err);
        }
    }

    #[test]
    fn create_table() {
        let query = "clients (id Int) (name Str) create";
        let mut database = Database {
            name: "database".to_string(),
            path: String::new(),
            tables: vec![],
        }; 
        let tokens = parse_query(query).unwrap();
        let result = execute_query(&tokens, &mut database);
        assert!(result.is_none());
        assert!(database.tables.len() == 1);
        let expected = Table {
            schema: TableSchema {
                name: "clients".to_string(),
                cols: vec![
                    Col {name: "id".to_string(), data_type: DataType::Int},
                    Col {name: "name".to_string(), data_type: DataType::Str},
                ],
            },
            rows: vec![],
        };
        assert!(expected == database.tables[0]);
    }

    #[test]
    fn drop_table() {
        let query = "clients drop";
        let mut database = Database {
            name: "database".to_string(),
            path: String::new(),
            tables: vec![
                Table {
                    schema: TableSchema {
                        name: "clients".to_string(),
                        cols: vec![],
                    },
                    rows: vec![],
                },
            ],
        }; 
        let tokens = parse_query(query).unwrap();
        let result = execute_query(&tokens, &mut database);
        assert!(result.is_none());
        assert!(database.tables.len() == 0);
    }

    #[test]
    fn save_and_load_database() {
        let expected = Database {
            name: "database".to_string(),
            path: String::new(),
            tables: vec![
                Table {
                    schema: TableSchema {
                        name: "table1".to_string(),
                        cols: vec![
                            Col {name: "id".to_string(), data_type: DataType::Int},
                            Col {name: "name".to_string(), data_type: DataType::Str},
                        ],
                    },
                    rows: vec![
                        vec![
                            WordType::Int(0),
                            WordType::Str("John".to_string()),
                        ],
                        vec![
                            WordType::Int(1),
                            WordType::Str("Dmitriy".to_string()),
                        ],
                    ],
                },
                Table {
                    schema: TableSchema {
                        name: "table2".to_string(),
                        cols: vec![
                            Col {name: "id".to_string(), data_type: DataType::Int},
                            Col {name: "name".to_string(), data_type: DataType::Str},
                            Col {name: "age".to_string(), data_type: DataType::Int},
                        ],
                    },
                    rows: vec![
                        vec![
                            WordType::Int(0),
                            WordType::Str("John".to_string()),
                            WordType::Int(25),
                        ],
                        vec![
                            WordType::Int(1),
                            WordType::Str("Dmitriy".to_string()),
                            WordType::Int(19),
                        ],
                    ],
                },
            ]
        };
        save_database_to("./src/tests_input/database", &expected).unwrap();
        let database = load_database_from("./src/tests_input/database").unwrap();
        'outer: for table in &expected.tables {
            for table1 in &database.tables {
                if table.schema.name == table1.schema.name {
                    assert!(table == table1);
                    continue 'outer;
                }
            }
            assert!(false);
        }
    }
}
