use std::io;
use std::io::Write;
use std::process::exit;

#[derive(Debug)]
struct Row {
    id: u64,
    name: String,
    age: u8,
}

type Table = Vec<Row>;

#[derive(Debug)]
enum OpType {
    SELECT,
}

#[derive(Debug)]
enum Token {
    OP(OpType),
    WORD(String),
}

fn try_parse_op(op: &str) -> Option<OpType> {
    match op {
        "select" => Some(OpType::SELECT),
        _ => None,  
    }
}

fn parse_querry(querry: &str) -> Vec<Token> {
    let mut tokens: Vec<Token> = vec![];
    for token in querry.split(' ') {
        if let Some(op) = try_parse_op(token) {
            tokens.push(Token::OP(op)); 
        } else {
            tokens.push(Token::WORD(String::from(token)));
        }
    }
    tokens
}

fn evaluate_querry(querry: &Vec<Token>, table: &Table) {
    let mut cols: Vec<&String> = vec![];
    for token in querry {
        match token {
            Token::OP(op) => {
                match op {
                    OpType::SELECT => {
                        for row in table {
                            for col in &cols {
                                match col.as_str() {
                                    "id" => print!("{0:>5} ", row.id),
                                    "name" => print!("{0:>5}", row.name),
                                    "age" => print!("{0:>5}", row.age),
                                    _ => unreachable!(),
                                }
                            }
                            println!("");
                        } 
                        cols.clear();
                    },
                    _ => unreachable!(),
                }
            },
            Token::WORD(value) => {
                cols.push(value);
            },
            _ => unreachable!(),
        }
    }
}

fn main() {
    let mut table: Vec<Row> = vec![
        Row { id: 0, name: String::from("John"), age: 28},
        Row { id: 1, name: String::from("Alexey"), age: 20},
        Row { id: 2, name: String::from("Vladimir"), age: 19},
    ];

    let mut quit = false;
    while !quit {
        print!("> "); 
        io::stdout().flush(); 
        
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
                if (buffer.trim() == "querry") {
                    eprintln!("ERROR: querry is not provided");
                    continue;    
                }
                
                let (_, querry) = buffer.split_once(' ').unwrap();
                let tokens = parse_querry(querry);
                println!("provided querry: {:?}", tokens);
                evaluate_querry(&tokens, &table);
            },
            "" => (),
            _ => println!("Unknown command: {buffer}"),
        }
    }
}
