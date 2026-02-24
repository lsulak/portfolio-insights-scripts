"""Edgar (SEC filings) Extraction Contracts - Data models and interfaces for the SEC extraction subsystem.

All public contracts (dataclasses and ABCs) live here, forming the
package's API surface for consumers and implementors alike.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from google import genai


# ==========================================
# EDGAR (SEC) FORM TYPES
# ==========================================
class EdgarFormType(str, Enum):
    """Edgar (SEC) filing form types supported by the extraction pipeline.

    Inherits from str so the enum value can be used directly as a string
    in file paths, dict keys, and log messages.
    """

    ANNUAL_REPORT = "10-K"
    QUARTERLY_REPORT = "10-Q"
    CURRENT_REPORT = "8-K"
    PROXY_STATEMENT = "DEF 14A"
    INSIDER_TRADING = "4"


# ==========================================
# DATA CONTRACTS
# ==========================================
@dataclass
class LocalEdgarDocument:
    """Represents a locally downloaded SEC filing."""

    ticker: str
    submission_year: int
    submission_order_for_the_year: int
    form_type: EdgarFormType
    file_path_raw: str
    file_path_ai_ready: Optional[str]
    mime_type: str


@dataclass
class AIHostedFile:
    """Reference to a file uploaded to Gemini's servers for inference."""

    file_uri: str
    file_name: str
    mime_type: str


# ==========================================
# INTERFACES
# ==========================================
class IEdgarFetcher(ABC):
    """Contract for fetching SEC filings from a data source."""

    @abstractmethod
    async def fetch_latest_filings(
        self, ticker: str, form_type: EdgarFormType, years_back: int
    ) -> Optional[List[LocalEdgarDocument]]:
        pass


class IAIFileManager(ABC):
    """Contract for managing document lifecycle on an AI provider's servers."""

    def __init__(self, client: genai.Client) -> None: ...

    @abstractmethod
    async def upload_for_inference(self, document: LocalEdgarDocument) -> AIHostedFile:
        pass

    @abstractmethod
    async def cleanup_remote_file(self, file_name: str) -> bool:
        pass


class IExtractorAgent(ABC):
    """Contract for AI-driven structured data extraction from documents."""

    def __init__(self, client: genai.Client, model_name: str) -> None: ...

    @abstractmethod
    async def generate_structured_dossier(self, ai_file: AIHostedFile, system_prompt: str) -> str:
        pass
