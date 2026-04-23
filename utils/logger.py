"""loguru 全局配置。集中配置避免各模块重复 setup。"""
import sys
from loguru import logger


def setup_logger(level: str = "INFO") -> None:
    # 覆盖默认 handler：stdout + 紧凑时间戳，便于 tail -f 观察
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        format=(
            "<green>{time:MM-DD HH:mm:ss}</green> "
            "<level>{level: <7}</level> "
            "<cyan>{name}:{line}</cyan> - {message}"
        ),
        enqueue=False,
    )


__all__ = ["logger", "setup_logger"]
