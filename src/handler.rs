use crate::{
    model::{AppState, QueryOptions, Todo, UpdateTodoSchema},
    response::{GenericResponse, SingleTodoResponse, TodoData, TodoListResponse},
};
use actix_web::{delete, get, patch, post, web, HttpResponse, Responder};
use chrono::prelude::*;
use scylla::IntoTypedRows;
use scylla::frame::value::CqlTimestamp;
use uuid::Uuid;

#[get("/healthchecker")]
async fn health_checker_handler() -> impl Responder {
    const MESSAGE: &str = "Build Simple CRUD API with Rust, Actix Web, and Scylla";

    let response_json = &GenericResponse {
        status: "success".to_string(),
        message: MESSAGE.to_string(),
    };
    HttpResponse::Ok().json(response_json)
}

#[get("/todos")]
pub async fn todos_list_handler(
    opts: web::Query<QueryOptions>,
    data: web::Data<AppState>,
) -> impl Responder {
    let limit = opts.limit.unwrap_or(10);
    
    let query = "SELECT id, title, content, completed, created_at, updated_at FROM todo_db.todos";
    
    let rows = match data.db.query(query, &[]).await {
        Ok(result) => result.rows,
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Database error: {}", e),
            };
            return HttpResponse::InternalServerError().json(error_response);
        }
    };

    let mut todos: Vec<Todo> = Vec::new();
    
    if let Some(rows) = rows {
        for row in rows.into_typed::<(String, String, String, bool, CqlTimestamp, CqlTimestamp)>() {
            if let Ok((id, title, content, completed, created_at, updated_at)) = row {
                todos.push(Todo {
                    id: Some(id),
                    title,
                    content,
                    completed: Some(completed),
                    createdAt: Some(DateTime::from_timestamp_millis(created_at.0).unwrap()),
                    updatedAt: Some(DateTime::from_timestamp_millis(updated_at.0).unwrap()),
                });
            }
        }
    }

    let offset = (opts.page.unwrap_or(1) - 1) * limit;
    let paginated_todos: Vec<Todo> = todos.into_iter().skip(offset).take(limit).collect();

    let json_response = TodoListResponse {
        status: "success".to_string(),
        results: paginated_todos.len(),
        todos: paginated_todos,
    };
    
    HttpResponse::Ok().json(json_response)
}

#[post("/todos")]
async fn create_todo_handler(
    body: web::Json<Todo>,
    data: web::Data<AppState>,
) -> impl Responder {
    // Debug: Log what we received
    println!("Received title: {}", body.title);
    println!("Received content: {}", body.content);
    
    let uuid_id = Uuid::new_v4().to_string();
    let datetime = Utc::now();
    let timestamp = CqlTimestamp(datetime.timestamp_millis());

    let title = body.title.clone();
    let content = body.content.clone();

    let check_query = "SELECT id FROM todo_db.todos WHERE title = ? ALLOW FILTERING";
    match data.db.query(check_query, (&title,)).await {
        Ok(result) => {
            if let Some(rows) = result.rows {
                if !rows.is_empty() {
                    let error_response = GenericResponse {
                        status: "fail".to_string(),
                        message: format!("Todo with title: '{}' already exists", title),
                    };
                    return HttpResponse::Conflict().json(error_response);
                }
            }
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Database error: {}", e),
            };
            return HttpResponse::InternalServerError().json(error_response);
        }
    }

    let insert_query = "INSERT INTO todo_db.todos (id, title, content, completed, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)";
    
    println!("Inserting: id={}, title={}, content={}", uuid_id, title, content);
    
    match data.db.query(
        insert_query,
        (&uuid_id, &title, &content, false, timestamp, timestamp)
    ).await {
        Ok(_) => {
            let todo = Todo {
                id: Some(uuid_id.clone()),
                title: title.clone(),
                content: content.clone(),
                completed: Some(false),
                createdAt: Some(datetime),
                updatedAt: Some(datetime),
            };

            println!("Successfully created todo with id: {}", uuid_id);

            let json_response = SingleTodoResponse {
                status: "success".to_string(),
                data: TodoData { todo },
            };

            HttpResponse::Ok().json(json_response)
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Failed to create todo: {}", e),
            };
            HttpResponse::InternalServerError().json(error_response)
        }
    }
}

#[get("/todos/{id}")]
async fn get_todo_handler(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
    let id = path.into_inner();
    
    let query = "SELECT id, title, content, completed, created_at, updated_at FROM todo_db.todos WHERE id = ?";
    
    match data.db.query(query, (&id,)).await {
        Ok(result) => {
            if let Some(rows) = result.rows {
                if let Some(row) = rows.into_typed::<(String, String, String, bool, CqlTimestamp, CqlTimestamp)>().next() {
                    if let Ok((id, title, content, completed, created_at, updated_at)) = row {
                        let todo = Todo {
                            id: Some(id),
                            title,
                            content,
                            completed: Some(completed),
                            createdAt: Some(DateTime::from_timestamp_millis(created_at.0).unwrap()),
                            updatedAt: Some(DateTime::from_timestamp_millis(updated_at.0).unwrap()),
                        };

                        let json_response = SingleTodoResponse {
                            status: "success".to_string(),
                            data: TodoData { todo },
                        };
                        
                        return HttpResponse::Ok().json(json_response);
                    }
                }
            }
            
            let error_response = GenericResponse {
                status: "fail".to_string(),
                message: format!("Todo with ID: {} not found", id),
            };
            HttpResponse::NotFound().json(error_response)
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Database error: {}", e),
            };
            HttpResponse::InternalServerError().json(error_response)
        }
    }
}

#[patch("/todos/{id}")]
async fn edit_todo_handler(
    path: web::Path<String>,
    body: web::Json<UpdateTodoSchema>,
    data: web::Data<AppState>,
) -> impl Responder {
    let id = path.into_inner();
    
    let select_query = "SELECT id, title, content, completed, created_at, updated_at FROM todo_db.todos WHERE id = ?";
    
    let existing_todo = match data.db.query(select_query, (&id,)).await {
        Ok(result) => {
            if let Some(rows) = result.rows {
                if let Some(row) = rows.into_typed::<(String, String, String, bool, CqlTimestamp, CqlTimestamp)>().next() {
                    if let Ok((id, title, content, completed, created_at, updated_at)) = row {
                        Some(Todo {
                            id: Some(id),
                            title,
                            content,
                            completed: Some(completed),
                            createdAt: Some(DateTime::from_timestamp_millis(created_at.0).unwrap()),
                            updatedAt: Some(DateTime::from_timestamp_millis(updated_at.0).unwrap()),
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Database error: {}", e),
            };
            return HttpResponse::InternalServerError().json(error_response);
        }
    };

    if existing_todo.is_none() {
        let error_response = GenericResponse {
            status: "fail".to_string(),
            message: format!("Todo with ID: {} not found", id),
        };
        return HttpResponse::NotFound().json(error_response);
    }

    let existing = existing_todo.unwrap();
    let datetime = Utc::now();
    let timestamp = CqlTimestamp(datetime.timestamp_millis());

    let new_title = body.title.clone().unwrap_or(existing.title.clone());
    let new_content = body.content.clone().unwrap_or(existing.content.clone());
    let new_completed = body.completed.unwrap_or(existing.completed.unwrap_or(false));

    let update_query = "UPDATE todo_db.todos SET title = ?, content = ?, completed = ?, updated_at = ? WHERE id = ?";
    
    match data.db.query(
        update_query,
        (&new_title, &new_content, new_completed, timestamp, &id)
    ).await {
        Ok(_) => {
            let todo = Todo {
                id: Some(id),
                title: new_title,
                content: new_content,
                completed: Some(new_completed),
                createdAt: existing.createdAt,
                updatedAt: Some(datetime),
            };

            let json_response = SingleTodoResponse {
                status: "success".to_string(),
                data: TodoData { todo },
            };

            HttpResponse::Ok().json(json_response)
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Failed to update todo: {}", e),
            };
            HttpResponse::InternalServerError().json(error_response)
        }
    }
}

#[delete("/todos/{id}")]
async fn delete_todo_handler(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
    let id = path.into_inner();
    
    let check_query = "SELECT id FROM todo_db.todos WHERE id = ?";
    match data.db.query(check_query, (&id,)).await {
        Ok(result) => {
            if let Some(rows) = result.rows {
                if rows.is_empty() {
                    let error_response = GenericResponse {
                        status: "fail".to_string(),
                        message: format!("Todo with ID: {} not found", id),
                    };
                    return HttpResponse::NotFound().json(error_response);
                }
            }
        }
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Database error: {}", e),
            };
            return HttpResponse::InternalServerError().json(error_response);
        }
    }

    let delete_query = "DELETE FROM todo_db.todos WHERE id = ?";
    
    match data.db.query(delete_query, (&id,)).await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(e) => {
            let error_response = GenericResponse {
                status: "error".to_string(),
                message: format!("Failed to delete todo: {}", e),
            };
            HttpResponse::InternalServerError().json(error_response)
        }
    }
}

pub fn config(conf: &mut web::ServiceConfig) {
    let scope = web::scope("/api")
        .service(health_checker_handler)
        .service(todos_list_handler)
        .service(create_todo_handler)
        .service(get_todo_handler)
        .service(edit_todo_handler)
        .service(delete_todo_handler);

    conf.service(scope);
}