use std::io;
use std::io::Write;
use std::io::Read;
use std::fs::File;
use std::process::exit;

#[derive(Debug)]
struct Row {
    id: i32,
    name: String,
    age: i32,
}

type Table = Vec<Row>;

#[derive(Debug, PartialEq, Clone)]
enum OpType {
    SELECT,
    INSERT,
    DELETE,
}

#[derive(Debug, PartialEq, Clone)]
enum Token {
    OP(OpType),
    INT(i32),
    STR(String),
}

#[derive(Debug)]
enum ColType {
    INT,
    STR,
    COUNT,
}

#[derive(Debug)]
struct TableSchema {
    name: String,
    cols: Vec<(String, ColType)>,
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
        _ => None,  
    }
}

fn parse_querry(querry: &str) -> Vec<Token> {
    let mut tokens: Vec<Token> = vec![];
    for word in querry.split(' ') {
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
                        for row in &mut *table {
                            for token in &tokens {
                                match token {
                                    Token::STR(_) => continue,
                                    _ => {
                                        eprintln!("ERROR: `select` operation can operate only strings");
                                        return;
                                    }
                                }
                            }
                            
                            for token in &tokens {
                                if let Token::STR(value) = token {
                                    match value.as_str() {
                                        "id" => print!("{0:>5} ", row.id),
                                        "name" => print!("{0:>15}", row.name),
                                        "age" => print!("{0:>5}", row.age),
                                        _ => unreachable!(),
                                    }
                                } else {
                                    unreachable!();
                                }
                            }
                            println!("");
                        } 
                        tokens.clear();
                    },
                    OpType::INSERT => {
                        if tokens.len() < 3 {
                            eprintln!("ERROR: not enaugh arguments for `insert` operation, provided {0} but needed 3", tokens.len());
                            return;
                        }

                        table.push(Row {
                            id: if let Token::INT(id) = tokens[0] { id } else { 
                                eprintln!("ERROR: invalid arguments type for `insert` operation");
                                return;
                            },
                            name: if let Token::STR(name) = &tokens[1] { String::from(name) } else { 
                                eprintln!("ERROR: invalid arguments type for `insert` operation");
                                return;
                            },
                            age: if let Token::INT(age) = tokens[2] { age } else { 
                                eprintln!("ERROR: invalid arguments type for `insert` operation");
                                return;
                            },
                        });

                        tokens.clear();
                    },
                    OpType::DELETE => {
                        if tokens.len() < 1 {
                            eprintln!("ERROR: not enaugh arguments for `delete` operation, provided 0 but needed 1");
                            return;
                        }

                        match tokens.pop().unwrap() {
                            Token::INT(id) => {
                                let mut rows_for_delete = vec![];
                                for (i, row) in table.iter().enumerate() {
                                    if row.id == id {
                                        rows_for_delete.push(i);
                                    }
                                }

                                for row in rows_for_delete {
                                    table.remove(row);
                                } 
                            },
                            other => {
                                eprintln!("ERROR: invalid argument type for `delete` operation, expected to be an integer but provided {other:?}");
                                return;
                            }
                        }
                    }
                }
            },
            _ => tokens.push(token.clone()),
        }
    }
}

fn main() {
    let mut table: Vec<Row> = vec![
        Row { id: 0, name: String::from("John"), age: 28},
        Row { id: 1, name: String::from("Alexey"), age: 20},
        Row { id: 2, name: String::from("Vladimir"), age: 19},
    ];

    let schema = parse_table_schema("./stuff.tbls");
    println!("{schema:?}");

    let mut quit = true;
    while !quit {
        print!("> "); 
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

        let command = buffer.as_str().split(' ').next().unwrap();
        match command {
            "exit" => quit = true,
            "querry" => {
                if buffer.trim() == "querry" {
                    eprintln!("ERROR: querry is not provided");
                    continue;    
                }
                
                let (_, querry) = buffer.split_once(' ').unwrap();
                let tokens = parse_querry(querry);
                println!("provided querry: {:?}", tokens);
                evaluate_querry(&tokens, &mut table);
            },
            "" => (),
            _ => println!("Unknown command: {buffer}"),
        }
    }
}
