from pydantic import BaseModel
from typing import Optional, List

class LoginRequest(BaseModel):
    username: str
    password: str

class CompanySchema(BaseModel):
    name: str
    gstin: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None

class UserCreate(BaseModel):
    username: str
    password: str
    name: str
    email: Optional[str] = None
    role: str = "viewer"
    branch: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    branch: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None

class ItemCreate(BaseModel):
    sku: str
    style_id: Optional[str] = None
    category: Optional[str] = None
    cost_price: Optional[float] = 0
    catalog_name: Optional[str] = None
    mrp: Optional[float] = 0
    location: Optional[str] = None
    uniware_sku: Optional[str] = None
    fba: Optional[str] = None
    sjit: Optional[str] = None
    fbf: Optional[str] = None
    top_fabric: Optional[str] = None
    bottom_fabric: Optional[str] = None
    dupatta_fabric: Optional[str] = None
    top_color: Optional[str] = None
    bottom_color: Optional[str] = None
    dupatta_color: Optional[str] = None
    top_size: Optional[str] = None
    bottom_size: Optional[str] = None
    dupatta_size: Optional[str] = None
    inner: Optional[str] = None
    inner_size: Optional[str] = None
    item_type: Optional[str] = None
    work: Optional[str] = None
    work_type: Optional[str] = None
    style: Optional[str] = None
    bottom_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    link: Optional[str] = None
    pose_1: Optional[str] = None
    pose_2: Optional[str] = None
    pose_3: Optional[str] = None
    pose_4: Optional[str] = None
    pose_5: Optional[str] = None
    pose_6: Optional[str] = None
    hsn_code: Optional[str] = None
    extra_1: Optional[str] = None
    extra_2: Optional[str] = None
    bullet_1: Optional[str] = None
    bullet_2: Optional[str] = None
    bullet_3: Optional[str] = None
    bullet_4: Optional[str] = None
    bullet_5: Optional[str] = None
    name: Optional[str] = None
    brand: Optional[str] = None
    tax_rate: Optional[float] = 0

class ItemUpdate(BaseModel):
    style_id: Optional[str] = None
    category: Optional[str] = None
    cost_price: Optional[float] = None
    catalog_name: Optional[str] = None
    mrp: Optional[float] = None
    location: Optional[str] = None
    uniware_sku: Optional[str] = None
    fba: Optional[str] = None
    sjit: Optional[str] = None
    fbf: Optional[str] = None
    top_fabric: Optional[str] = None
    bottom_fabric: Optional[str] = None
    dupatta_fabric: Optional[str] = None
    top_color: Optional[str] = None
    bottom_color: Optional[str] = None
    dupatta_color: Optional[str] = None
    top_size: Optional[str] = None
    bottom_size: Optional[str] = None
    dupatta_size: Optional[str] = None
    inner: Optional[str] = None
    inner_size: Optional[str] = None
    item_type: Optional[str] = None
    work: Optional[str] = None
    work_type: Optional[str] = None
    style: Optional[str] = None
    bottom_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    link: Optional[str] = None
    pose_1: Optional[str] = None
    pose_2: Optional[str] = None
    pose_3: Optional[str] = None
    pose_4: Optional[str] = None
    pose_5: Optional[str] = None
    pose_6: Optional[str] = None
    hsn_code: Optional[str] = None
    extra_1: Optional[str] = None
    extra_2: Optional[str] = None
    bullet_1: Optional[str] = None
    bullet_2: Optional[str] = None
    bullet_3: Optional[str] = None
    bullet_4: Optional[str] = None
    bullet_5: Optional[str] = None
    name: Optional[str] = None
    is_active: Optional[bool] = None

class PhysicalReturnRow(BaseModel):
    date:         Optional[str] = ""
    channel:      Optional[str] = ""
    order_no:     Optional[str] = ""
    awb:          Optional[str] = ""
    courier:      Optional[str] = ""
    putway:       Optional[str] = ""
    sku_r:        Optional[str] = ""
    to_rma:       Optional[str] = ""
    remark:       Optional[str] = ""
    putaway_code: Optional[str] = ""

class PhysicalReturnSync(BaseModel):
    rows: List[PhysicalReturnRow]
    replace: bool = True
