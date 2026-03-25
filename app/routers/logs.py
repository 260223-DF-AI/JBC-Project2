import os

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import PlainTextResponse

from ..utils.logger import get_logger

router = APIRouter(
    prefix="/logs",
    tags=["logs"]
)

@router.get("/")
async def get_logs():
    """
    Return plaintext contents from the logs/log.log file
    """

    logger = get_logger(__name__)
    logger.info("Beginning get_logs endpoint execution")

    log_path = "logs/log.log"
    if not os.path.exists(log_path):
        return PlainTextResponse(content="Log file not found yet.",
                                 status_code=status.HTTP_404_NOT_FOUND
                                 )
    
    with open(log_path, "r") as f:
        # return the whole file
        return PlainTextResponse(content=f.read(),
                                 status_code=status.HTTP_200_OK
                                 )