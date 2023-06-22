
full_username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")
user_name = full_username.split("@")[0]
