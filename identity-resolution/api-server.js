/**
 * Customer Identity Resolution API Server
 * =====================================
 * 
 * Purpose: Backend service for processing visitor identities and CRM lead matching
 * Handles ADF/XML lead parsing, session matching, and Segment CDP integration
 * 
 * Architecture:
 * - Express.js API server (deployed on Vercel/AWS Lambda)
 * - PostgreSQL database for identity storage and matching
 * - Email webhook processing for CRM lead ingestion
 * - Segment API integration for unified customer profiles
 * 
 * Business Impact:
 * - Connects website behavior to offline sales (attribution)
 * - Enables personalized marketing based on interest signals
 * - Provides sales team with customer journey context
 */

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { body, validationResult } from 'express-validator';
import winston from 'winston';
import { DatabaseService } from './services/database.js';
import { SegmentService } from './services/segment.js';
import { ADFParser } from './utils/adf-parser.js';

// Configure structured logging
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'identity-resolver.log' })
    ]
});

// Initialize Express application
const app = express();
const PORT = process.env.PORT || 3000;

// Security middleware
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'", "'unsafe-inline'"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            connectSrc: ["'self'", "https://api.segment.io"]
        }
    }
}));

// Rate limiting to prevent abuse
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    message: 'Too many requests from this IP, please try again later',
    standardHeaders: true,
    legacyHeaders: false
});
app.use('/api/', limiter);

// CORS configuration for website integration
app.use(cors({
    origin: [
        'https://your-dealership-website.com',
        'https://www.your-dealership-website.com',
        'http://localhost:3000' // Development only
    ],
    credentials: true,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Visitor-Tracking']
}));

// Body parsing middleware
app.use(express.json({ limit: '10mb' })); // Large limit for email content
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Initialize services
const dbService = new DatabaseService();
const segmentService = new SegmentService();
const adfParser = new ADFParser();

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        version: process.env.npm_package_version || '1.0.0'
    });
});

/**
 * Visitor Identity Capture Endpoint
 * =================================
 * Receives visitor tracking data from website JavaScript
 * Stores identity information for later CRM matching
 */
app.post('/api/visitor-mapping',
    // Input validation
    [
        body('segment_anonymous_id').notEmpty().withMessage('Segment ID is required'),
        body('digital_retailing_session_id').optional().isString(),
        body('landing_page').isURL().withMessage('Invalid landing page URL'),
        body('timestamp').isISO8601().withMessage('Invalid timestamp format')
    ],
    async (req, res) => {
        try {
            // Check for validation errors
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                logger.warn('Visitor mapping validation failed', {
                    errors: errors.array(),
                    ip: req.ip
                });
                return res.status(400).json({
                    error: 'Validation failed',
                    details: errors.array()
                });
            }

            // Extract and clean visitor data
            const visitorData = {
                segment_anonymous_id: req.body.segment_anonymous_id,
                digital_retailing_session_id: req.body.digital_retailing_session_id,
                
                // Marketing attribution
                utm_source: req.body.utm_source,
                utm_medium: req.body.utm_medium,
                utm_campaign: req.body.utm_campaign,
                utm_term: req.body.utm_term,
                utm_content: req.body.utm_content,
                gclid: req.body.gclid,
                fbclid: req.body.fbclid,
                
                // Session context
                referrer: req.body.referrer,
                landing_page: req.body.landing_page,
                page_title: req.body.page_title,
                user_agent: req.headers['user-agent'],
                ip_address: req.ip,
                
                // Browser fingerprint for fraud detection
                browser_fingerprint: req.body.browser_fingerprint,
                
                // Timestamps
                timestamp: req.body.timestamp,
                session_start: req.body.session_start
            };

            // Store visitor session in database
            const sessionId = await dbService.saveVisitorSession(visitorData);

            logger.info('Visitor session captured', {
                sessionId,
                segmentId: visitorData.segment_anonymous_id,
                utm_source: visitorData.utm_source,
                ip: req.ip
            });

            res.json({
                success: true,
                message: 'Visitor identity captured successfully',
                sessionId: sessionId,
                timestamp: new Date().toISOString()
            });

        } catch (error) {
            logger.error('Error capturing visitor session', {
                error: error.message,
                stack: error.stack,
                body: req.body
            });

            res.status(500).json({
                error: 'Internal server error',
                message: 'Failed to capture visitor identity'
            });
        }
    }
);

/**
 * CRM Lead Processing Endpoint
 * ===========================
 * Processes ADF/XML lead data from email webhooks
 * Matches leads to visitor sessions and sends to Segment
 */
app.post('/api/process-lead',
    // Input validation for email webhook
    [
        body('subject').notEmpty().withMessage('Email subject is required'),
        body('from').isEmail().withMessage('Valid sender email is required'),
        body('email').notEmpty().withMessage('Email content is required')
    ],
    async (req, res) => {
        try {
            // Validate input
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                logger.warn('Lead processing validation failed', {
                    errors: errors.array(),
                    subject: req.body.subject
                });
                return res.status(400).json({
                    error: 'Validation failed',
                    details: errors.array()
                });
            }

            logger.info('Processing incoming lead email', {
                subject: req.body.subject,
                from: req.body.from,
                timestamp: new Date().toISOString()
            });

            // Extract email content from webhook payload
            const emailContent = extractEmailContent(req.body.email);
            
            // Parse ADF/XML lead data
            const leadData = await adfParser.parseADFFromEmail(emailContent);
            
            if (!leadData) {
                logger.warn('No valid ADF/XML found in email', {
                    subject: req.body.subject,
                    contentPreview: emailContent.substring(0, 200)
                });
                return res.status(400).json({
                    error: 'No valid ADF/XML lead data found in email'
                });
            }

            logger.info('Successfully parsed ADF lead data', {
                leadId: leadData.leadId,
                email: leadData.email,
                sessionId: leadData.sessionId,
                vehicleInterest: leadData.vehicleInterest
            });

            // Attempt to match lead to visitor session
            let matchedSession = null;
            if (leadData.sessionId) {
                matchedSession = await dbService.findVisitorBySessionId(leadData.sessionId);
            }

            // Fallback matching by email/phone if no session match
            if (!matchedSession && (leadData.email || leadData.phone)) {
                matchedSession = await dbService.findVisitorByContact(
                    leadData.email, 
                    leadData.phone,
                    leadData.timestamp
                );
            }

            if (matchedSession) {
                // Complete lead with attribution data
                const enrichedLead = {
                    ...leadData,
                    segment_anonymous_id: matchedSession.segment_anonymous_id,
                    attribution: {
                        utm_source: matchedSession.utm_source,
                        utm_medium: matchedSession.utm_medium,
                        utm_campaign: matchedSession.utm_campaign,
                        utm_term: matchedSession.utm_term,
                        utm_content: matchedSession.utm_content,
                        gclid: matchedSession.gclid,
                        fbclid: matchedSession.fbclid,
                        referrer: matchedSession.referrer,
                        landing_page: matchedSession.landing_page
                    }
                };

                // Save complete lead to database
                await dbService.saveEnrichedLead(enrichedLead);

                // Send to Segment with full attribution
                await segmentService.trackLeadSubmission(enrichedLead);

                logger.info('Lead successfully matched and sent to Segment', {
                    leadId: lea
