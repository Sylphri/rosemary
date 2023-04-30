use std::io;
use std::io::Write;
use std::io::Read;
use std::fs::File;
use std::fs;
use std::fmt;
use std::process::exit;

mod tests;

#[derive(Debug, Clone)]
struct TableSchema {
    name: String,
    cols: Vec<(String, ColType)>,
}

// TODO: think about redisign of token system
type Row = Vec<WordType>;

#[derive(Debug)]
struct Table {
    schema: TableSchema,
    rows: Vec<Row>,
}

impl fmt::Display for Table {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        assert!(ColType::Count as u8 == 2, "Exhaustive ColType handling in Table::fmt()");
        for (name, col_type) in &self.schema.cols {
            match col_type {
                ColType::Int => print!("{name:>5}"),
                ColType::Str => print!("{name:>20}"),
                _ => unreachable!(),
            }
        }
        println!();
        for row in &self.rows {
            for word in row {
                match word {
                    WordType::Int(value) => print!("{value:>5}"),
                    WordType::Str(value) => print!("{value:>20}"),
                }
            }
            println!();
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum OpType {
    Select,
    Insert,
    Delete,
    FilterAnd,
    FilterOr,
    Equal,
    NotEqual,
    Less,
    More,
    Count,
}

#[derive(Debug, PartialEq, Clone)]
enum WordType {
    Int(i32),
    Str(String),
}

// TODO: Introduce a sized string type
#[derive(Debug, PartialEq, Clone)]
enum Token {
    Op(OpType),
    Word(WordType),
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum ColType {
    Int,
    Str,
    Count,
}

fn cmp_word_with_col(word: &WordType, col_type: ColType) -> bool {
    assert_eq!(ColType::Count as u8, 2);
    match word {
        WordType::Int(_) => return col_type == ColType::Int,
        WordType::Str(_) => return col_type == ColType::Str,
    } 
}

fn try_parse_col_type(col_type: &str) -> Option<ColType> {
    assert_eq!(ColType::Count as u8, 2);
    match col_type {
        "Int" => Some(ColType::Int),
        "Str" => Some(ColType::Str),
        _ => None,
    }
} 

fn parse_table_schema(file_path: &str) -> Result<TableSchema, String> {
    let file = File::open(file_path);
    if let Err(err) = file {
        return Err(format!("ERROR: unable to open the file {file_path}: {err}"));
    } 
    let mut file = file.unwrap();

    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        return Err(format!("ERROR: unable to read from the file {file_path}: {err}"));
    }

    let mut cols = vec![];
    let mut lines = content.lines();
    let name = match lines.next() {
        Some(value) => value.trim(),
        None => return Err(format!("ERROR: table name not provided in a file {file_path}")),
    };

    if name.len() == 0 {
        return Err(format!("ERROR: table name can't be empty: {file_path}"));
    }

    for (i, line) in lines.enumerate() {
        let (name, type_name) = match line.split_once(':') {
            Some((name, type_name)) => (name.trim(), type_name.trim()),
            None => return Err(format!("ERROR: invalid format for column at line {} in a file: {}", i + 1, file_path)),
        };

        if name.len() == 0 {
            return Err(format!("ERROR: empty column name at line {} in a file {}", i + 1, file_path));
        }

        for (col_name, _) in &cols {
            if col_name == name {
                return Err(format!("ERROR: column with name '{}' already exists in table scheme: {}", col_name, file_path));
            } 
        }

        if let Some(value) = try_parse_col_type(type_name) {
            cols.push((String::from(name), value));
        } else {
            return Err(format!("ERROR: unknown column type at line {} in a file {}", i + 1, file_path));
        } 
    }

    Ok(TableSchema { name: name.to_string(), cols })
}

fn try_parse_op(op: &str) -> Option<OpType> {
    assert!(OpType::Count as u8 == 9, "Exhaustive OpType handling in try_parse_op()");
    match op {
        "select" => Some(OpType::Select),
        "insert" => Some(OpType::Insert),
        "delete" => Some(OpType::Delete),
        "filter-and" => Some(OpType::FilterAnd),
        "filter-or" => Some(OpType::FilterOr),
        "==" => Some(OpType::Equal),
        "!=" => Some(OpType::NotEqual),
        ">" => Some(OpType::More),
        "<" => Some(OpType::Less),
        _ => None,  
    }
}

fn parse_query(query: &str) -> Option<Vec<Token>> {
    let mut tokens: Vec<Token> = vec![];
    let mut query = query.clone();
    loop {
        query = query.trim_start();
        if query.len() == 0 { break; }
        if query.bytes().next().unwrap() == b'"' {
            query = &query[1..]; 
            if let Some(end) = query.find('"') {
                tokens.push(Token::Word(WordType::Str(String::from(&query[0..end]))));
                query = &query[end+1..];
            } else {
                eprintln!("ERROR: unclosed string literal in a query");
                return None;
            }
        } else {
            let end = query.find(char::is_whitespace).unwrap_or_else(|| {
                unreachable!();
            });
            let word = &query[0..end];
            query = &query[end..];
            if let Some(op) = try_parse_op(word) {
                tokens.push(Token::Op(op)); 
            } else if let Ok(value) = word.parse::<i32>() {
                tokens.push(Token::Word(WordType::Int(value)));
            } else {
                tokens.push(Token::Word(WordType::Str(String::from(word))));
            }
        }
    }
    Some(tokens)
}

#[derive(Debug)]
struct Condition {
    idx: usize,
    value: WordType,
    op: OpType,
}

fn logical_op_check(op: OpType, words: &[WordType], temp_table: &Table) -> Option<Condition> {
    assert!(OpType::Count as u8 == 9, "Exhaustive OpType handling in logical_op_check()");
    let op_sym = match op {
        OpType::Equal => "==",
        OpType::NotEqual => "!=",
        OpType::Less => "<",
        OpType::More => ">",
        _ => unreachable!(),
    };

    if words.len() < 2 {
        eprintln!("ERROR: not enaugh arguments for `{op_sym}` operation, provided {0} but needed 2", words.len());
        return None;
    }
    
    let mut col = String::new();
    match &words[words.len() - 2] {
        WordType::Str(value) => col = value.clone(),
        _ => {
            eprintln!("ERROR: invalid argument for `{op_sym}` operation, expected string but found {0:?}", col);
            return None;
        },
    }

    let mut idx = -1;
    for (i, (col_name, _)) in temp_table.schema.cols.iter().enumerate() {
        if *col_name == col {
            idx = i as i32;
            break;
        }
    }

    if idx < 0 {
        eprintln!("ERROR: no such column `{0}` in table `{1}`", col, temp_table.schema.name);
        return None;
    }

    let value = &words[words.len() - 1];
    if !cmp_word_with_col(&value, temp_table.schema.cols[idx as usize].1) {
        eprintln!("ERROR: invalid argument for `{op_sym}` operation expected type {0:?} but found type {1:?}", temp_table.schema.cols[idx as usize], value);
        return None;
    }
    
    Some(Condition {
        idx: idx as usize,
        value: value.clone(),
        op: op,
    })
}

fn filter_condition<T: PartialOrd>(a: &T, b: &T, condition: OpType) -> bool {
    match condition {
        OpType::Equal => *a != *b,
        OpType::NotEqual => *a == *b,
        OpType::Less => *a >= *b,
        OpType::More => *a <= *b,
        _ => unreachable!(),
    }
}

fn execute_query(query: &Vec<Token>, table: &mut Table) -> Option<Table> {
    let mut words: Vec<WordType> = vec![];
    let mut conditions: Vec<Condition> = vec![];
    let mut temp_table = Table {
        schema: TableSchema {
            name: String::from("temp"),
            cols: vec![],
        },
        rows: vec![],
    };
    
    for (token_id, token) in query.iter().enumerate() {
        match token {
            Token::Op(op) => {
                match op {
                    OpType::Select => {
                        if words.len() == 0 {
                            eprintln!("ERROR: no arguments provided for `select` operation");
                            return None;
                        }
                        let mut row_idxs = vec![];
                        'outer: for word in &words {
                            match word {
                                WordType::Str(value) => {
                                    if value == "*" {
                                        row_idxs.append(&mut (0..table.schema.cols.len()).collect::<Vec<usize>>());
                                        continue;
                                    }

                                    for (i, (col_name, _)) in table.schema.cols.iter().enumerate() {
                                        if col_name == value {
                                            row_idxs.push(i);
                                            continue 'outer;
                                        }
                                    }
                                    eprintln!("ERROR: non existing column `{0}` in table `{1}`", value, table.schema.name);
                                    return None;
                                },
                                _ => {
                                    eprintln!("ERROR: `select` operation can operate only strings");
                                    return None;
                                }
                            }
                        }

                        let mut schema = TableSchema {
                            name: String::from("temp"),
                            cols: vec![],
                        };
                        
                        for idx in &row_idxs {
                            schema.cols.push(table.schema.cols[*idx].clone());
                        }

                        temp_table = Table {
                            schema,
                            rows: vec![],
                        };

                        for row in &table.rows {
                            let mut temp_row = vec![];
                            for idx in &row_idxs {
                                temp_row.push(row[*idx as usize].clone());
                            }
                            temp_table.rows.push(temp_row);
                        }
                        words.clear();
                    },
                    OpType::Insert => {
                        let col_count = table.schema.cols.len(); 
                        if words.len() < col_count {
                            eprintln!("ERROR: not enaugh arguments for `insert` operation, provided {0} but needed {1}", words.len(), col_count);
                            return None;
                        }

                        for (i, word) in words[words.len() - col_count..words.len()].iter().enumerate() {
                            if !cmp_word_with_col(word, table.schema.cols[i].1) {
                                eprintln!("ERROR: argument type don't match the column type, argumnet {0:?}, column {1:?}", word, table.schema.cols[i].1);
                                return None;
                            }
                        }

                        table.rows.push(words[words.len() - col_count..words.len()].to_vec());
                        words.clear();
                    },
                    OpType::Delete => {
                        let mut rows_to_delete = vec![];
                        for (i, row) in table.rows.iter().enumerate() {
                            let mut filtered_cols = 0;
                            for condition in &conditions {
                                assert!(OpType::Count as u8 == 9, "Exhaustive logic OpTypes handling");
                                match &row[condition.idx] {
                                    WordType::Int(value) => {
                                        if let WordType::Int(cond_value) = &condition.value {
                                            if !filter_condition(value, cond_value, condition.op) {
                                                filtered_cols += 1;
                                            }
                                        } else {
                                            unreachable!();
                                        }
                                    },
                                    WordType::Str(value) => {
                                        if let WordType::Str(cond_value) = &condition.value {
                                            if !filter_condition(value, cond_value, condition.op) {
                                                filtered_cols += 1;
                                            }
                                        } else {
                                            unreachable!();
                                        }
                                    }
                                }
                            }
                            if filtered_cols == conditions.len() {
                                rows_to_delete.push(i);
                            }
                        }

                        let mut deleted = 0;
                        for row in rows_to_delete {
                            table.rows.remove(row - deleted);
                            deleted += 1;
                        }       
                        conditions.clear();
                    },
                    OpType::FilterAnd => {
                        let mut filtered_rows = vec![];
                        for row in &temp_table.rows {
                            let mut filtered = false;
                            for condition in &conditions {
                                assert!(OpType::Count as u8 == 9, "Exhaustive logic OpTypes handling");
                                match &row[condition.idx] {
                                    WordType::Int(value) => {
                                        if let WordType::Int(cond_value) = &condition.value {
                                            filtered = filter_condition(value, cond_value, condition.op);
                                        } else {
                                            unreachable!();
                                        }
                                    },
                                    WordType::Str(value) => {
                                        if let WordType::Str(cond_value) = &condition.value {
                                            filtered = filter_condition(value, cond_value, condition.op);
                                        } else {
                                            unreachable!();
                                        }
                                    }
                                }
                                if filtered { break; }
                            }

                            if !filtered {
                                filtered_rows.push(row.clone());
                            }
                        }

                        temp_table.rows = filtered_rows; 
                        conditions.clear();
                    },
                    OpType::FilterOr => {
                        let mut filtered_rows = vec![];
                        for row in &temp_table.rows {
                            let mut filtered_count = 0;
                            for condition in &conditions {
                                assert!(OpType::Count as u8 == 9, "Exhaustive logic OpTypes handling");
                                match &row[condition.idx] {
                                    WordType::Int(value) => {
                                        if let WordType::Int(cond_value) = &condition.value {
                                            if filter_condition(value, cond_value, condition.op) { filtered_count += 1; }
                                        } else {
                                            unreachable!();
                                        }
                                    },
                                    WordType::Str(value) => {
                                        if let WordType::Str(cond_value) = &condition.value {
                                            if filter_condition(value, cond_value, condition.op) { filtered_count += 1; }
                                        } else {
                                            unreachable!();
                                        }
                                    }
                                }
                            }

                            if filtered_count < conditions.len() {
                                filtered_rows.push(row.clone());
                            }
                        }

                        temp_table.rows = filtered_rows; 
                        conditions.clear();
                    },
                    op @ OpType::Equal | op @ OpType::NotEqual | op @ OpType::Less | op @ OpType::More => {
                        let mut curr_table = &temp_table;
                        let mut query = query[token_id + 1..].iter();
                        while let Some(token) = query.next() {
                            match token {
                                Token::Word(_) => (),
                                Token::Op(op) => {
                                    if OpType::Delete == *op {
                                        curr_table = &table;
                                        break;
                                    }
                                }
                            }      
                        } 
                        
                        if let Some(condition) = logical_op_check(*op, &words, &curr_table) {
                            conditions.push(condition);
                            words.pop();
                            words.pop();
                        } else {
                            return None;
                        }
                    },
                    OpType::Count => unreachable!(),
                }
            },
            Token::Word(word) => words.push(word.clone()),
        }
    }

    if words.len() > 0 {
        eprintln!("WARNING: {0} unused words in the stack", words.len());
    }
    if conditions.len() > 0 {
        eprintln!("WARNING: {0} unused conditions in the stack", conditions.len());
    }
    Some(temp_table)
}

fn read_from_file(table: &mut Table) {
    let file_path = format!("./tables/{0}.tbl", table.schema.name);
    let mut file = File::open(&file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to open the file {file_path}: {err}");
        exit(1);
    });

    let mut row_len = 0;
    for (_, col_type) in &table.schema.cols {
        match col_type {
            ColType::Int => row_len += 4,
            ColType::Str => row_len += 50,
            _ => unreachable!(),
        }
    }
    
    let file_len = fs::metadata(&file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: can't get size of the file {file_path}: {err}");
        exit(1); 
    }).len();
    
    if file_len % row_len != 0 {
        eprintln!("ERROR: incorrect file format in {file_path}");
        exit(1);
    }

    let mut i32_buf: [u8; 4] = [0; 4];
    let mut str_buf: [u8; 50] = [0; 50];
    for _ in 0..file_len / row_len {
        let mut row: Row = vec![];
        for (_, col_type) in &table.schema.cols {
            match col_type {
                ColType::Int => {
                    file.read(&mut i32_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to read from file {file_path}: {err}");
                        exit(1);
                    });
                    
                    row.push(WordType::Int(i32::from_ne_bytes(i32_buf)));
                },
                ColType::Str => {
                    file.read(&mut str_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to read from file {file_path}: {err}");
                        exit(1);
                    });
                    
                    let str_len = str_buf.iter().position(|&x| x == 0).unwrap_or(50);
                    row.push(WordType::Str(String::from_utf8_lossy(&str_buf[0..str_len]).to_string()));
                },
                _ => unreachable!(),
            }
        }
        table.rows.push(row);
    }  
}

fn save_to_file(table: Table) {
    let file_path = format!("./tables/{0}.tbl", table.schema.name);
    let mut file = File::create(&file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to create a file for table: {err}");
        exit(1);
    });
    
    for row in &table.rows {
        for word in row {
            match word {
                WordType::Int(value) => {
                    file.write_all(&value.to_ne_bytes()).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to write to the file {file_path}: {err}");
                        exit(1);
                    });     
                },
                WordType::Str(value) => {
                    let mut value = &value[0..];
                    if value.len() > 50 {
                        eprintln!("WARNING: string length must be less or equal to 50, only first 50 characters will be saved");
                        value = &value[0..50];
                    }
                    let mut str_buf: [u8; 50] = [0; 50];
                    str_buf[0..value.len()].clone_from_slice(&value.as_bytes());
                    file.write_all(&str_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to write to the file {file_path}: {err}");
                        exit(1);
                    });     
                },
            }
        } 
    }
}

#[derive(PartialEq)]
enum Mode {
    Cmd,
    Query,
    MlQuery,
}

// TODO: Make some tests
fn main() {
    let file_path = "./tables/stuff.tbls";
    let schema = parse_table_schema(file_path);
    if let Err(err) = schema {
        eprintln!("{}", err);
        exit(1);
    }
    
    let mut table = Table {
       schema: schema.unwrap().clone(),
       rows: vec![],
    };

    read_from_file(&mut table);

    let mut quit = false;
    let mut mode = Mode::Cmd;
    let mut query = String::new();
    while !quit {
        match mode {
            Mode::Cmd => print!("> "),
            Mode::Query => print!("querry > "),
            Mode::MlQuery => print!("querry : "),
        } 
        
        io::stdout().flush().unwrap_or_else(|err| {
            eprintln!("ERROR: unable to flush stdout: {err}");
            exit(1);
        });
        
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap_or_else(|err| {
            eprintln!("ERROR: unable to read the line: {err}");
            exit(1);
        });

        // TODO: Add the command history
        match mode {
            Mode::Cmd => {
                let command = buffer.as_str().split_ascii_whitespace().next();
                match command {
                    Some("exit") => quit = true,
                    Some("query") => mode = Mode::Query,
                    None => (),
                    Some(value) => println!("Unknown command: {value}"),
                }
            },
            Mode::Query | Mode::MlQuery => {
                for c in buffer.bytes() {
                    if c == b'(' {
                        mode = Mode::MlQuery;
                    } else if c == b')' {
                        mode = Mode::Query;
                    }
                }
                buffer = buffer.replace("(", "");
                buffer = buffer.replace(")", "");
                query.push_str(&buffer);
                if mode == Mode::MlQuery {
                    continue;
                }

                match query.as_str().trim() {
                    "exit" => mode = Mode::Cmd,
                    _ => {
                        if let Some(tokens) = parse_query(query.as_str()) {
                            if let Some(result_table) = execute_query(&tokens, &mut table) {
                                print!("{result_table}");
                            }
                        }
                    },
                }
                query.clear();
            },
        }
    }

    save_to_file(table);
}
