from motor.motor_asyncio import AsyncIOMotorClient
from config import settings
import certifi

ca = certifi.where()

client = AsyncIOMotorClient(
    settings.MONGODB_URI,
    tls=True,
    tlsCAFile=ca,
    serverSelectionTimeoutMS=30000,
    connectTimeoutMS=30000,
    socketTimeoutMS=60000,
    maxPoolSize=50,
    retryWrites=True,
)

db = client.sentinel_db
customers = db.customers
transactions = db.transactions
devices = db.devices
merchants = db.merchants
