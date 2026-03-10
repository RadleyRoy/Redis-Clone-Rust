use crate::database::db::Database;

pub async fn command_parser(cmd: &str, db: &Database) -> Result<String, String> {
    let splitted_command: Vec<&str> = cmd.split_whitespace().collect();

    match splitted_command.as_slice() {
        ["SET", key, value] => {
            db.set(key.to_string(), value.to_string(), None).await;
            Ok("+OK\r\n".to_string())
        }
        ["SET", key, value, "EXP", ttl_str] => {
            let ttl = ttl_str.parse::<u64>().expect("Invalid TTL value");
            db.set(key.to_string(), value.to_string(), Some(ttl)).await;
            Ok("+OK\r\n".to_string())
        }
        ["GET", key] => match db.get(key).await {
            Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
            None => Ok("$-1\r\n".to_string()),
        },
        ["DEL", key] => {
            if db.delete(key).await {
                Ok("$1\r\n:1\r\n".to_string())
            } else {
                Ok("$-1\r\n".to_string())
            }
        }
        ["LPUSH", key, value] => {
            db.lpush(key.to_string(), value.to_string()).await;
            Ok("+OK\r\n".to_string())
        }
        ["RPUSH", key, value] => {
            db.rpush(key.to_string(), value.to_string()).await;
            Ok("+OK\r\n".to_string())
        }
        ["LPOP", key] => {
            if let Some(value) = db.lpop(key).await {
                Ok(format!("${}\r\n{}\r\n", value.len(), value))
            } else {
                Ok("$-1\r\n".to_string())
            }
        }
        ["RPOP", key] => {
            if let Some(value) = db.rpop(key).await {
                Ok(format!("${}\r\n{}\r\n", value.len(), value))
            } else {
                Ok("$-1\r\n".to_string())
            }
        }
        ["LRANGE", start_str, end_str, key] => {
            let start = start_str.parse::<usize>().expect("Invalid start index");
            let end = end_str.parse::<usize>().expect("Invalid end index");
            if let Some(values) = db.lrange(start, end, key).await {
                let mut response = format!("*{}\r\n", values.len());
                for value in values {
                    response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
                Ok(response)
            } else {
                Ok("$-1\r\n".to_string())
            }
        }
        ["SADD", key, value] => {
            if db.sadd(key.to_string(), value.to_string()).await {
                Ok("$1\r\n:1\r\n".to_string())
            } else {
                Ok("$0\r\n:0\r\n".to_string())
            }
        }
        ["SREM", key, value] => {
            if db.srem(key, value).await {
                Ok("$1\r\n:1\r\n".to_string())
            } else {
                Ok("$0\r\n:0\r\n".to_string())
            }
        }
        ["SMEMBERS", key] => match db.smembers(key).await {
            Some(members) => {
                let mut response = format!("*{}\r\n", members.len());
                for member in members {
                    response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                }
                Ok(response)
            }
            None => Ok("$-1\r\n".to_string()),
        },
        ["SISMEMBER", key, value] => {
            if db.sismember(key, value).await {
                Ok("$1\r\n:1\r\n".to_string())
            } else {
                Ok("$0\r\n:0\r\n".to_string())
            }
        }

        _ => Ok("-ERR\r\nUnknown command".to_string()),
    }
}
