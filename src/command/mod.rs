use crate::database::Database;

pub async fn command_parser(cmd: &str, db: &Database) -> Result<String, String>
{
   let splitted_command: Vec<&str> = cmd.split_whitespace().collect();
   
   match splitted_command.as_slice()
   {
        ["SET", key, value] => 
        {
            db.set(key.to_string(), value.to_string(), None).await;
            Ok("+OK\r\n".to_string())
        }
        ["SET", key, value, "EXP", ttl_str] => 
        {
            let ttl = ttl_str.parse::<u64>().expect("Invalid TTL value");
            db.set(key.to_string(), value.to_string(), Some(ttl)).await;
            Ok("+OK\r\n".to_string())
        }
        ["GET", key] => 
        {
            match db.get(key).await
            {
                Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                None => Ok("$-1\r\n".to_string()),
            }
        }
        ["DEL", key] => 
        {
            if db.delete(key).await
            {
                Ok("$1\r\n:1\r\n".to_string())
            }
            else
            {
                Ok("$-1\r\n".to_string())
            }
        }
        _ => Ok("-ERR\r\nUnknown command".to_string()),
   }
}