import os
import json
import time
import logging
import asyncio
import shutil
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# 初始化应用 (禁用自动生成的文档)
app = FastAPI(docs_url=None, redoc_url=None)

# 创建必要的目录
Path("files").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# 初始化模板
templates = Jinja2Templates(directory=".")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/share_tool.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据模型
class User(BaseModel):
    id: str
    name: str
    ip: str
    last_active: float

class FileInfo(BaseModel):
    name: str
    size: int
    upload_time: float
    uploader: str
    uploader_ip: str

# 初始化数据存储
def get_message_filename() -> str:
    """生成基于时间戳的消息文件名"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"messages_{timestamp}.json"

def load_data(file: str) -> Dict:
    if not Path(file).exists():
        return {}
    
    # 尝试不同的编码方式
    encodings = ["utf-8", "latin1", None]  # None 表示二进制模式
    
    for encoding in encodings:
        try:
            if encoding is None:
                # 使用二进制模式读取，然后尝试解码
                with open(file, "rb") as f:
                    content = f.read()
                    # 检查 BOM 标记
                    if content.startswith(b'\xef\xbb\xbf'):  # UTF-8 BOM
                        content = content[3:]
                        decoded = content.decode('utf-8')
                    elif content.startswith(b'\xff\xfe') or content.startswith(b'\xfe\xff'):  # UTF-16 BOM
                        decoded = content.decode('utf-16')
                    else:
                        # 尝试以 latin1 处理（可以处理所有字节值）
                        decoded = content.decode('latin1')
                    return json.loads(decoded)
            else:
                with open(file, "r", encoding=encoding) as f:
                    return json.load(f)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to load {file} with encoding {encoding}: {e}")
            # 继续尝试下一种编码
            continue
    
    # 如果所有尝试都失败，返回空字典
    logger.error(f"Could not load {file} with any encoding")
    return {}

def save_data(file: str, data: Dict):
    try:
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save data to {file}: {e}")
        # 尝试创建备份文件
        backup_file = f"{file}.bak"
        try:
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            logger.info(f"Created backup file: {backup_file}")
        except Exception as backup_error:
            logger.error(f"Failed to create backup file: {backup_error}")

# 全局状态
users_db = load_data("users.json")
files_db = load_data("file.json")
messages_filename = get_message_filename()
messages_db = load_data(messages_filename)
active_connections = set()

# 工具函数
def get_client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"

def log_activity(action: str, details: str, ip: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} [{ip}] - {action} - {details}"
    logger.info(log_message)

# WebSocket连接管理器
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        try:
            await websocket.accept()
            self.active_connections.append(websocket)
        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
            return None

    def disconnect(self, websocket: WebSocket):
        try:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        except Exception as e:
            logger.error(f"WebSocket断开连接时出错: {e}")

    async def broadcast(self, message: str):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except (WebSocketDisconnect, ConnectionResetError) as e:
                logger.warning(f"广播消息时发现断开的连接: {e}")
                disconnected.append(connection)
            except Exception as e:
                logger.error(f"广播消息时出错: {e}")
                disconnected.append(connection)
        
        # 清理断开的连接
        for connection in disconnected:
            self.disconnect(connection)

manager = ConnectionManager()

# 路由
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    client_ip = get_client_ip(request)
    return templates.TemplateResponse("home.html", {"request": request, "ip": client_ip})

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), username: str = Form(...)):
    client_ip = get_client_ip(request)
    
    # 保存文件
    file_path = f"files/{file.filename}"
    file_size = 0
    try:
        contents = await file.read()
        file_size = len(contents)
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        log_activity("UPLOAD_FAIL", f"{file.filename} - {str(e)}", client_ip)
        raise HTTPException(status_code=500, detail=str(e))
    
    # 更新文件数据库
    file_info = {
        "name": file.filename,
        "size": file_size,
        "upload_time": time.time(),
        "uploader": username,
        "uploader_ip": client_ip
    }
    files_db[file.filename] = file_info
    save_data("file.json", files_db)
    
    # 更新用户数据库
    users_db[client_ip] = {
        "id": client_ip,
        "name": username,
        "ip": client_ip,
        "last_active": time.time()
    }
    save_data("users.json", users_db)
    
    log_activity("UPLOAD", f"{file.filename} ({file_size} bytes)", client_ip)
    await manager.broadcast("update_files")
    return {"status": "success", "filename": file.filename}

@app.post("/upload_chunk")
async def upload_chunk(
    request: Request,
    file: UploadFile = File(...),
    chunk_number: int = Form(...),
    total_chunks: int = Form(...),
    filename: str = Form(...),
    username: str = Form(...)
):
    # 创建chunks目录
    chunks_dir = Path("files/chunks")
    chunks_dir.mkdir(exist_ok=True, parents=True)
    
    client_ip = get_client_ip(request)
    chunk_filename = f"{filename}.part{chunk_number}"
    chunk_path = chunks_dir / chunk_filename
    
    try:
        contents = await file.read()
        chunk_path.write_bytes(contents)
        
        # 如果是最后一个分片，合并所有分片
        if chunk_number == total_chunks - 1:
            final_path = Path("files") / filename
            with open(final_path, "wb") as final_file:
                for i in range(total_chunks):
                    part_path = chunks_dir / f"{filename}.part{i}"
                    if not part_path.exists():
                        raise HTTPException(status_code=400, detail=f"分片{i}丢失")
                    final_file.write(part_path.read_bytes())
                    part_path.unlink()  # 删除分片
            
            file_size = final_path.stat().st_size
            file_info = {
                "name": filename,
                "size": file_size,
                "upload_time": time.time(),
                "uploader": username,
                "uploader_ip": client_ip
            }
            files_db[filename] = file_info
            save_data("file.json", files_db)
            log_activity("UPLOAD", f"{filename} ({file_size} bytes)", client_ip)
            await manager.broadcast("update_files")
                
        return {"status": "success", "chunk": chunk_number}
    except Exception as e:
        log_activity("UPLOAD_CHUNK_FAIL", f"{filename} chunk {chunk_number} - {str(e)}", client_ip)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = f"files/{filename}"
    if not Path(file_path).exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename)

@app.delete("/delete/{filename}")
async def delete_file(request: Request, filename: str):
    client_ip = get_client_ip(request)
    file_path = f"files/{filename}"
    chunks_dir = Path("files/chunks")
    
    try:
        # 检查文件是否存在
        if not Path(file_path).exists():
            log_activity("DELETE_FAIL", f"{filename} - File not found", client_ip)
            raise HTTPException(status_code=404, detail="File not found")
        
        # 检查权限 (只有上传者可以删除)
        if filename not in files_db:
            log_activity("DELETE_FAIL", f"{filename} - File info not found in database", client_ip)
            raise HTTPException(status_code=404, detail="File information not found")
            
        if files_db[filename]["uploader_ip"] != client_ip:
            log_activity("DELETE_FAIL", f"{filename} - Unauthorized deletion attempt", client_ip)
            raise HTTPException(status_code=403, detail="Only the uploader can delete this file")
        
        # 删除主文件
        os.remove(file_path)
        
        # 删除相关的临时分片文件(如果存在)
        if chunks_dir.exists():
            for chunk in chunks_dir.glob(f"{filename}.part*"):
                try:
                    chunk.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete chunk file {chunk}: {e}")
        
        # 从数据库移除
        del files_db[filename]
        save_data("file.json", files_db)
        
        log_activity("DELETE_SUCCESS", f"{filename} deleted by uploader", client_ip)
        await manager.broadcast("update_files")
        return {"status": "success", "message": "File deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        log_activity("DELETE_ERROR", f"{filename} - {str(e)}", client_ip)
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")

@app.get("/list_files")
async def list_files():
    return {"files": list(files_db.values())}

@app.get("/list_users")
async def list_users():
    # 清理不活跃用户 (30分钟未活动)
    inactive_threshold = time.time() - 1800
    active_users = {k:v for k,v in users_db.items() if v["last_active"] > inactive_threshold}
    save_data("users.json", active_users)
    return {"users": list(active_users.values())}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                manager.disconnect(websocket)
                break
            except ConnectionResetError:
                logger.warning("远程主机强制关闭了连接")
                manager.disconnect(websocket)
                break
            except Exception as e:
                logger.error(f"WebSocket处理消息时出错: {e}")
                manager.disconnect(websocket)
                break
    except Exception as e:
        logger.error(f"WebSocket连接发生错误: {e}")
        manager.disconnect(websocket)

@app.post("/send_message")
async def send_message(request: Request, message: str = Form(...), username: str = Form(...)):
    client_ip = get_client_ip(request)
    
    # 创建消息记录
    message_data = {
        "id": str(time.time()),
        "content": message,
        "sender": username,
        "sender_ip": client_ip,
        "timestamp": time.time()
    }
    
    # 保存到消息数据库
    if "messages" not in messages_db:
        messages_db["messages"] = []
    messages_db["messages"].append(message_data)
    save_data(messages_filename, messages_db)
    
    # 更新用户数据库
    users_db[client_ip] = {
        "id": client_ip,
        "name": username,
        "ip": client_ip,
        "last_active": time.time()
    }
    save_data("users.json", users_db)
    
    log_activity("MESSAGE", message, client_ip)
    await manager.broadcast("new_message")
    return {"status": "success"}

@app.get("/list_messages")
async def list_messages():
    if "messages" not in messages_db:
        return {"messages": []}
    
    # 只返回最近的100条消息
    messages = messages_db.get("messages", [])
    recent_messages = sorted(messages, key=lambda x: x["timestamp"], reverse=True)[:100]
    return {"messages": recent_messages}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)