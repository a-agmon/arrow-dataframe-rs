# arrow-dataframe-rs

A failed attempt to create a dataframe that willl wrap arrow record batch. It fails because its just too slowing down

given that records here is an arrow RecordsBatch, this can be used like this:

```rust
  for row in BatchDataFrame::new(records)?.rows {
      let user_id = row.get("user_id").unwrap().extract_text()?;
      let acc_id = row.get("acc_id").unwrap().extract_text()?;
      let email = row.get("email").unwrap().extract_text()?;
      let department = row.get("department").unwrap().extract_text()?;
      let created_at = row.get("created_at").unwrap().extract_timestamp()?;
      users.push(User::new(user_id, acc_id, email, department));
  }
```
