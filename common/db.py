from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


class DatabaseConflictError(Exception):
    pass


def commit_or_raise_conflict(
    db: Session,
    *,
    message: str = "database conflict",
) -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise DatabaseConflictError(message) from exc
