"""
FastAPI web server for Lead Analysis API
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional
import logging
from datetime import datetime

from app.services.lead_analyzer import LeadAnalyzerService
from app.config import get_config, validate_config
from app.logger import get_logger

# Initialize FastAPI app
app = FastAPI(
    title="Bitrix24 Lead Analyzer API",
    description="API for automated lead analysis and status updates",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure based on your needs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize logger
logger = get_logger("FastAPI")

# Global analyzer service instance
analyzer_service: Optional[LeadAnalyzerService] = None


class AnalysisResponse(BaseModel):
    """Response model for analysis operations"""
    status: str
    message: str
    batch_id: Optional[str] = None
    total_leads: int = 0
    success_rate: float = 0.0
    leads_updated: int = 0
    processing_time: Optional[float] = None
    details: Optional[Dict[str, Any]] = None


class WebhookPayload(BaseModel):
    """Webhook payload model"""
    leadId: str
    event: str
    timestamp: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    services: Dict[str, bool]
    configuration: Dict[str, Any]


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global analyzer_service

    logger.info("Starting Bitrix24 Lead Analyzer API")

    try:
        # Validate configuration
        if not validate_config():
            raise RuntimeError("Invalid configuration")

        # Initialize analyzer service
        analyzer_service = LeadAnalyzerService()

        # Test services
        health = analyzer_service.check_health()
        failed_services = [service for service, status in health.items() if not status]

        if failed_services:
            logger.warning(f"Some services are not healthy: {failed_services}")
        else:
            logger.info("All services are healthy")

        logger.info("API startup completed successfully")

    except Exception as e:
        logger.error(f"Failed to start API: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global analyzer_service

    logger.info("Shutting down Bitrix24 Lead Analyzer API")

    if analyzer_service:
        try:
            analyzer_service.close()
        except Exception as e:
            logger.error(f"Error closing analyzer service: {e}")


@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint"""
    return {
        "service": "Bitrix24 Lead Analyzer API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        services_health = analyzer_service.check_health()
        config = get_config()

        return HealthResponse(
            status="healthy" if all(services_health.values()) else "degraded",
            timestamp=datetime.now().isoformat(),
            services=services_health,
            configuration={
                "check_interval_hours": config.scheduler.check_interval_hours,
                "max_concurrent_leads": config.scheduler.max_concurrent_leads,
                "junk_statuses": config.lead_status.junk_statuses
            }
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {e}")


@app.post("/analyze/new-leads", response_model=AnalysisResponse)
async def analyze_new_leads(background_tasks: BackgroundTasks, dry_run: bool = False):
    """Analyze new leads added since last check"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        logger.info(f"Starting new leads analysis (dry_run={dry_run})")

        # Run analysis
        batch_result = analyzer_service.analyze_new_leads(dry_run=dry_run)

        return AnalysisResponse(
            status="success",
            message=f"Analysis completed successfully",
            batch_id=batch_result.batch_id,
            total_leads=batch_result.total_leads,
            success_rate=batch_result.success_rate,
            leads_updated=batch_result.leads_updated,
            processing_time=batch_result.total_processing_time,
            details={
                "successful_analyses": batch_result.successful_analyses,
                "failed_analyses": batch_result.failed_analyses,
                "leads_kept": batch_result.leads_kept,
                "leads_skipped": batch_result.leads_skipped
            }
        )

    except Exception as e:
        logger.error(f"New leads analysis failed: {e}")
        return AnalysisResponse(
            status="error",
            message=f"Analysis failed: {str(e)}",
            total_leads=0,
            success_rate=0.0
        )


@app.post("/analyze/all-junk", response_model=AnalysisResponse)
async def analyze_all_junk_leads(dry_run: bool = False):
    """Analyze all existing junk leads"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        logger.info(f"Starting all junk leads analysis (dry_run={dry_run})")

        # Run analysis
        batch_result = analyzer_service.analyze_all_junk_leads(dry_run=dry_run)

        return AnalysisResponse(
            status="success",
            message=f"Analysis completed successfully",
            batch_id=batch_result.batch_id,
            total_leads=batch_result.total_leads,
            success_rate=batch_result.success_rate,
            leads_updated=batch_result.leads_updated,
            processing_time=batch_result.total_processing_time,
            details={
                "successful_analyses": batch_result.successful_analyses,
                "failed_analyses": batch_result.failed_analyses,
                "leads_kept": batch_result.leads_kept,
                "leads_skipped": batch_result.leads_skipped
            }
        )

    except Exception as e:
        logger.error(f"All junk leads analysis failed: {e}")
        return AnalysisResponse(
            status="error",
            message=f"Analysis failed: {str(e)}",
            total_leads=0,
            success_rate=0.0
        )


@app.post("/analyze/lead/{lead_id}", response_model=AnalysisResponse)
async def analyze_single_lead(lead_id: str, dry_run: bool = False):
    """Analyze a specific lead by ID"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        logger.info(f"Starting analysis for lead {lead_id} (dry_run={dry_run})")

        # Run analysis
        result = analyzer_service.analyze_lead_by_id(lead_id, dry_run=dry_run)

        if not result:
            return AnalysisResponse(
                status="error",
                message="Lead not found",
                total_leads=0,
                success_rate=0.0
            )

        return AnalysisResponse(
            status="success" if result.is_successful else "error",
            message=f"Analysis completed: {result.action.value if result.action else 'unknown'}",
            total_leads=1,
            success_rate=1.0 if result.is_successful else 0.0,
            leads_updated=1 if result.requires_update else 0,
            processing_time=result.processing_time,
            details={
                "lead_id": result.lead_id,
                "action": result.action.value if result.action else None,
                "reason": result.reason.value if result.reason else None,
                "original_status": result.original_status,
                "original_junk_status": result.original_junk_status,
                "new_status": result.new_status,
                "new_junk_status": result.new_junk_status,
                "unsuccessful_calls_count": result.unsuccessful_calls_count,
                "transcription_success_rate": result.transcription_success_rate,
                "error": result.error_message
            }
        )

    except Exception as e:
        logger.error(f"Single lead analysis failed: {e}")
        return AnalysisResponse(
            status="error",
            message=f"Analysis failed: {str(e)}",
            total_leads=0,
            success_rate=0.0
        )


@app.post("/webhook/lead-updated")
async def webhook_lead_updated(payload: WebhookPayload, background_tasks: BackgroundTasks):
    """Webhook endpoint for lead updates from Bitrix24"""
    try:
        logger.info(f"Received webhook for lead {payload.leadId}, event: {payload.event}")

        # Process webhook in background if it's a relevant event
        if payload.event in ["ONADD", "ONUPDATE"]:
            background_tasks.add_task(process_lead_webhook, payload.leadId)

        return {
            "status": "received",
            "leadId": payload.leadId,
            "event": payload.event,
            "message": "Webhook processed successfully"
        }

    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {e}")


async def process_lead_webhook(lead_id: str):
    """Process lead webhook in background"""
    if not analyzer_service:
        logger.error("Analyzer service not initialized")
        return

    try:
        logger.info(f"Processing webhook for lead {lead_id}")

        # Analyze the updated lead
        result = analyzer_service.analyze_lead_by_id(lead_id, dry_run=False)

        if result and result.is_successful:
            logger.info(f"Webhook processing completed for lead {lead_id}: {result.action.value}")
        else:
            logger.warning(f"Webhook processing failed for lead {lead_id}")

    except Exception as e:
        logger.error(f"Error processing webhook for lead {lead_id}: {e}")


@app.get("/statistics")
async def get_statistics():
    """Get system statistics"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        stats = analyzer_service.get_statistics()
        return stats

    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting statistics: {e}")


@app.post("/test/pipeline")
async def test_pipeline():
    """Test the complete analysis pipeline"""
    if not analyzer_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        pipeline_ok = analyzer_service.test_analysis_pipeline()

        return {
            "status": "success" if pipeline_ok else "failed",
            "message": "Pipeline test completed" if pipeline_ok else "Pipeline test failed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Pipeline test failed: {e}")
        return {
            "status": "error",
            "message": f"Pipeline test error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }


if __name__ == "__main__":
    import uvicorn

    # Run the server
    uvicorn.run(
        "api_server:app",
        host="127.0.0.1",
        port=8000,
        log_level="info",
        reload=False
    )