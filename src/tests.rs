#[cfg(test)]
mod tests {
    use crate::*;
    
    #[test]
    fn valid_table_schema() {
        let schema = parse_table_schema("./src/tests_input/valid_table_schema.tbls");
        if let Err(ref err) = schema {
            assert!(false, "{}", err);
        }
        let schema = schema.unwrap();
        assert!(schema.name == "TestTable");
        assert!(schema.cols.len() == 3);
        assert!(schema.cols[0].0 == "id");
        assert!(schema.cols[0].1 == ColType::Int);
        assert!(schema.cols[1].0 == "name");
        assert!(schema.cols[1].1 == ColType::Str);
        assert!(schema.cols[2].0 == "age");
        assert!(schema.cols[2].1 == ColType::Int);
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
}
