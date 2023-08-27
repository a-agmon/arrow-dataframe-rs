 pub async fn get_users_df(&self) -> anyhow::Result<Vec<User>> {
        let records = self
            .db
            .query_fetch_batches(
                "select user_id, acc_id, email, department, created_at from users limit 100",
            )
            .await?;
        let mut users = Vec::new();
        for row in BatchDataFrame::new(records)?.rows {
            let user_id = row.get("user_id").unwrap().extract_text()?;
            let acc_id = row.get("acc_id").unwrap().extract_text()?;
            let email = row.get("email").unwrap().extract_text()?;
            let department = row.get("department").unwrap().extract_text()?;
            let created_at = row.get("created_at").unwrap().extract_timestamp()?;
            users.push(User::new(user_id, acc_id, email, department));
        }

        Ok(users)
    }
