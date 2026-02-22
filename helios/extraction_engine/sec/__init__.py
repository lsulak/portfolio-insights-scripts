"""SEC Extraction Subsystem - Downloads, cleans, and summarizes SEC filings.

Usage:
    from helios.extraction_engine.sec import SECExtractionPipeline
    from helios.extraction_engine.sec import LocalSECDocument, GeminiHostedFile
"""

from helios.extraction_engine.sec.api import (
    EdgarFormType,
    LocalEdgarDocument,
    AIHostedFile,
    IEdgarFetcher,
    IAIFileManager,
    IExtractorAgent,
)
from helios.extraction_engine.sec.fetcher import EdgarFetcher
from helios.extraction_engine.sec.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.sec.ai_summarizer import GeminiFileManager, ExtractorAgent
from helios.extraction_engine.sec.extraction_workflow import EdgarExtractionPipeline

__all__ = [
    # Enums
    "EdgarFormType",
    # Data contracts
    "LocalEdgarDocument",
    "AIHostedFile",
    # Interfaces
    "IEdgarFetcher",
    "IAIFileManager",
    "IExtractorAgent",
    # Implementations
    "EdgarFetcher",
    "EdgarDocumentCleaner",
    "GeminiFileManager",
    "ExtractorAgent",
    # Workflow
    "EdgarExtractionPipeline",
]
