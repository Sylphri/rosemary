use std::io;
use std::io::Write;
use std::io::Read;
use std::fs::File;
use std::fs;
use std::process::exit;

#[derive(Debug, Clone)]
struct TableSchema {
    name: String,
    cols: Vec<(String, ColType)>,
}

// TODO: think about redisign of token system
type Row = Vec<Token>;

#[derive(Debug)]
struct Table {
    schema: TableSchema,
    rows: Vec<Row>,
}

#[derive(Debug, PartialEq, Clone)]
enum OpType {
    SELECT,
    INSERT,
    DELETE,
    CLEAR,
}

#[derive(Debug, PartialEq, Clone)]
enum Token {
    OP(OpType),
    INT(i32),
    STR(String),
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum ColType {
    INT,
    STR,
    COUNT,
}

fn cmp_token_with_col(token: &Token, col_type: ColType) -> bool {
    assert_eq!(ColType::COUNT as u8, 2);
    match token {
        Token::INT(_) => return col_type == ColType::INT,
        Token::STR(_) => return col_type == ColType::STR,
        _ => unreachable!(),
    } 
}

fn try_parse_col_type(col_type: &str) -> Option<ColType> {
    assert_eq!(ColType::COUNT as u8, 2);
    match col_type {
        "INT" => Some(ColType::INT),
        "STR" => Some(ColType::STR),
        _ => None,
    }
} 

fn parse_table_schema(file_path: &str) -> TableSchema {
    let mut file = File::open(file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to open the file {file_path}: {err}");
        exit(1);
    });

    let mut content = String::new();
    file.read_to_string(&mut content).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to read from the file {file_path}: {err}");
        exit(1);
    });

    let mut cols = vec![];
    let mut lines = content.lines();
    let name = lines.next().unwrap_or_else(|| {
        eprintln!("ERROR: table name not provided in a file {file_path}");
        exit(1);
    }).to_string();

    if name.contains(":") {
        eprintln!("ERROR: table name can't contain a ':': {file_path}");
        exit(1);
    }

    for (i, line) in lines.enumerate() {
        let (name, type_name) = line.split_once(':').unwrap_or_else(|| {
            eprintln!("ERROR: invalid format for column at line {i} in a file {file_path}");
            exit(1);
        });

        if name.len() == 0 {
            eprintln!("ERROR: empty column name at line {i} in a file {file_path}");
            exit(1);
        }

        if let Some(value) = try_parse_col_type(type_name) {
            cols.push((String::from(name), value));
        } else {
            eprintln!("ERROR: unknown column type at line {i} in a file {file_path}");
            exit(1);
        } 
    }

    TableSchema { name, cols }
}

fn try_parse_op(op: &str) -> Option<OpType> {
    match op {
        "select" => Some(OpType::SELECT),
        "insert" => Some(OpType::INSERT),
        "delete" => Some(OpType::DELETE),
        "clear"  => Some(OpType::CLEAR),
        _ => None,  
    }
}

fn parse_querry(querry: &str) -> Vec<Token> {
    let mut tokens: Vec<Token> = vec![];
    for word in querry.split_ascii_whitespace() {
        if let Some(op) = try_parse_op(word) {
            tokens.push(Token::OP(op)); 
        } else if let Ok(value) = word.parse::<i32>() {
            tokens.push(Token::INT(value));
        } else {
            tokens.push(Token::STR(String::from(word)));
        }
    }
    tokens
}

fn evaluate_querry(querry: &Vec<Token>, table: &mut Table) {
    let mut tokens: Vec<Token> = vec![];
    for token in querry {
        match token {
            Token::OP(op) => {
                match op {
                    OpType::SELECT => {
                        let mut row_idxs = vec![];
                        'outer: for token in &tokens {
                            match token {
                                Token::STR(value) => {
                                    if value == "*" {
                                        row_idxs.append(&mut (0..table.schema.cols.len()).collect::<Vec<usize>>());
                                        continue 'outer;
                                    }

                                    for (i, (col_name, _)) in table.schema.cols.iter().enumerate() {
                                        if col_name == value {
                                            row_idxs.push(i);
                                            continue 'outer;
                                        }
                                    }
                                    eprintln!("ERROR: non existing column `{0}` in table `{1}`", value, table.schema.name);
                                    return;
                                },
                                _ => {
                                    eprintln!("ERROR: `select` operation can operate only strings");
                                    return;
                                }
                            }
                        }

                        for row in &table.rows {
                            for idx in &row_idxs {
                                match &row[*idx as usize] {
                                    Token::INT(value) => print!("{value:>5}"),
                                    Token::STR(value) => print!("{value:>20}"),
                                    _ => unreachable!(),
                                }
                            }
                            println!();
                        }
                    },
                    OpType::INSERT => {
                        let col_count = table.schema.cols.len(); 
                        if tokens.len() < col_count {
                            eprintln!("ERROR: not enaugh arguments for `insert` operation, provided {0} but needed {1}", tokens.len(), col_count);
                            return;
                        }

                        for (i, token) in tokens[tokens.len() - col_count..tokens.len()].iter().enumerate() {
                            if !cmp_token_with_col(token, table.schema.cols[i].1) {
                                eprintln!("ERROR: argument type don't match the column type, argumnet {0:?}, column {1:?}", token, table.schema.cols[i].1);
                                return;
                            }
                        }

                        table.rows.push(tokens[tokens.len() - col_count..tokens.len()].to_vec());
                    },
                    OpType::DELETE => {
                        if tokens.len() < 2 {
                            eprintln!("ERROR: not enaugh arguments for `delete` operation, provided {0} but needed 2", tokens.len());
                            return;
                        }

                        let value_token = tokens.pop().unwrap();
                        let target_token = tokens.pop().unwrap();
                        let mut idx = -1;
                        match target_token {
                            Token::STR(value_token) => {
                                for (i, (col_name, _)) in table.schema.cols.iter().enumerate() {
                                    if *col_name == value_token {
                                        idx = i as i32;
                                        break;
                                    }
                                }
                                
                                if idx < 0 {
                                    eprintln!("ERROR: no such column `{0}` in table `{1}`", value_token, table.schema.name);
                                    return;
                                }
                            },
                            _ => {
                                eprintln!("ERROR: first argument for `delete` operation must be a string but provided {0:?}", target_token);
                                return;
                            }
                        }        

                        if !cmp_token_with_col(&value_token, table.schema.cols[idx as usize].1) {
                            eprintln!("ERROR: value_token for compare that was provided for `delete` operation has type {0:?} but column {1} has type {2:?}", value_token, table.schema.cols[idx as usize].0, table.schema.cols[idx as usize].1);
                            return;
                        }

                        let mut rows_to_delete = vec![];
                        for (i, row) in table.rows.iter().enumerate() {
                            if value_token == row[idx as usize] {
                                rows_to_delete.push(i); 
                            }
                        }

                        let mut deleted = 0;
                        for row in rows_to_delete {
                            table.rows.remove(row - deleted);
                            deleted += 1;
                        }       
                    },
                    OpType::CLEAR => tokens.clear(),
                }
            },
            _ => tokens.push(token.clone()),
        }
    }
}

fn read_from_file(table: &mut Table) {
    let file_path = format!("{0}.tbl", table.schema.name);
    let mut file = File::open(&file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to open the file {file_path}: {err}");
        exit(1);
    });

    let mut row_len = 0;
    for (_, col_type) in &table.schema.cols {
        match col_type {
            ColType::INT => row_len += 4,
            ColType::STR => row_len += 50,
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
                ColType::INT => {
                    file.read(&mut i32_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to read from file {file_path}: {err}");
                        exit(1);
                    });
                    
                    row.push(Token::INT(i32::from_ne_bytes(i32_buf)));
                },
                ColType::STR => {
                    file.read(&mut str_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to read from file {file_path}: {err}");
                        exit(1);
                    });
                    
                    let str_len = str_buf.iter().position(|&x| x == 0).unwrap_or(50);
                    row.push(Token::STR(String::from_utf8_lossy(&str_buf[0..str_len]).to_string()));
                },
                _ => unreachable!(),
            }
        }
        table.rows.push(row);
    }  
}

fn save_to_file(table: Table) {
    let file_path = format!("{0}.tbl", table.schema.name);
    let mut file = File::create(&file_path).unwrap_or_else(|err| {
        eprintln!("ERROR: unable to create a file for table: {err}");
        exit(1);
    });
    
    for row in &table.rows {
        for token in row {
            match token {
                Token::INT(value) => {
                    file.write_all(&value.to_ne_bytes()).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to write to the file {file_path}: {err}");
                        exit(1);
                    });     
                },
                Token::STR(value) => {
                    assert!(value.len() <= 50, "ERROR: string literals can't be longer then 50 characters");
                    let mut str_buf: [u8; 50] = [0; 50];
                    str_buf[0..value.len()].clone_from_slice(&value.as_bytes());
                    file.write_all(&str_buf).unwrap_or_else(|err| {
                        eprintln!("ERROR: unable to write to the file {file_path}: {err}");
                        exit(1);
                    });     
                },
                _ => unreachable!(),
            }
        } 
    }
}

// TODO: Make some tests
fn main() {
    let file_path = "./stuff.tbls";
    let schema = parse_table_schema(file_path);
    let mut table = Table {
       schema: schema.clone(),
       rows: vec![],
    };

    read_from_file(&mut table);

    let mut quit = false;
    let mut querry_mode = false;
    while !quit {
        if querry_mode { 
            print!("querry > "); 
        } else { 
            print!("> "); 
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
        buffer.pop();

        // TODO: Add the command history
        if querry_mode {
            match buffer.as_str() {
                "exit" => querry_mode = false,
                "" => (),
                _ => {
                    let tokens = parse_querry(buffer.as_str());
                    evaluate_querry(&tokens, &mut table);
                },
            }
            continue;
        } 
        
        let command = buffer.as_str().trim_start().split(' ').next().unwrap();
        match command {
            "exit" => quit = true,
            "querry" => querry_mode = true,
            "" => (),
            _ => println!("Unknown command: {command}"),
        }
    }

    save_to_file(table);
}
