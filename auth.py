from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from database import get_db
import models

SECRET_KEY = "rajnandini-fashion-secret-key-2024"
ALGORITHM = "HS256"
security = HTTPBearer()

def create_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(hours=24)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        user = db.query(models.User).filter(models.User.username == username).first()
        if not user or not user.is_active:
            raise HTTPException(status_code=401, detail="Invalid user")
        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
