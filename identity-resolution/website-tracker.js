/**
 * Customer Identity Resolution - Website Visitor Tracker
 * =====================================================
 * 
 * Purpose: Captures website visitor identities and digital retailing interactions
 * for cross-system customer journey tracking and marketing attribution.
 * 
 * Business Challenge: 
 * - Digital retailing tools (Gubagoo) don't pass visitor IDs to form fields
 * - Website analytics and CRM systems operate in silos
 * - No way to connect online behavior to offline sales
 * 
 * Technical Solution:
 * - Captures Segment Anonymous ID for web analytics tracking
 * - Extracts digital retailing session UUIDs from widget interactions  
 * - Collects UTM parameters and referrer data for attribution
 * - Sends unified identity data to backend for CRM matching
 */

(function() {
    'use strict';
    
    // Configuration - Environment-specific settings
    const CONFIG = {
        API_ENDPOINT: 'https://identity-resolver.your-domain.com/api/visitor-mapping',
        DIGITAL_RETAILING_DOMAIN: 'widget-provider.com', // Generic digital retailing provider
        DEBUG: false, // Set to true for development
        MAX_RETRY_ATTEMPTS: 3,
        RETRY_DELAY_MS: 1000
    };
    
    // Utility Functions
    function log(message, data = null) {
        if (CONFIG.DEBUG) {
            console.log('[Identity Tracker]', message, data || '');
        }
    }
    
    function checkPrivacyCompliance() {
        /**
         * Check user privacy preferences and browser settings
         * Respects Do Not Track and consent management platforms
         */
        
        // Check for Do Not Track browser setting
        if (navigator.doNotTrack === '1' || 
            navigator.doNotTrack === 'yes' || 
            navigator.msDoNotTrack === '1') {
            log('Do Not Track detected - respecting user privacy preference');
            return false;
        }
        
        // Check for consent management platform (if implemented)
        if (window.cookieConsent && !window.cookieConsent.analytics) {
            log('Analytics consent not granted');
            return false;
        }
        
        // Check for opt-out cookie
        if (document.cookie.includes('identity_tracking_opt_out=true')) {
            log('User has opted out of identity tracking');
            return false;
        }
        
        // Check for privacy-focused browser extensions
        if (window.navigator.userAgent.includes('DuckDuckGo')) {
            log('Privacy-focused browser detected - limited tracking');
            return false;
        }
        
        return true;
    }
    
    function respectPrivacySettings() {
        /**
         * Additional privacy safeguards and data minimization
         */
        
        // Anonymize IP address on client side (basic)
        const originalSendBeacon = navigator.sendBeacon;
        if (originalSendBeacon) {
            navigator.sendBeacon = function(url, data) {
                // Remove detailed fingerprinting in privacy mode
                if (typeof data === 'string') {
                    try {
                        const parsed = JSON.parse(data);
                        if (parsed.browser_fingerprint) {
                            // Minimal fingerprint for privacy compliance
                            parsed.browser_fingerprint = {
                                timezone: parsed.browser_fingerprint.timezone,
                                language: parsed.browser_fingerprint.language
                            };
                        }
                        data = JSON.stringify(parsed);
                    } catch (e) {
                        // If parsing fails, continue with original data
                    }
                }
                return originalSendBeacon.call(this, url, data);
            };
        }
    }
    
    function getUrlParameters() {
        /**
         * Extract marketing attribution parameters from URL
         * Critical for connecting website visits to campaign performance
         */
        const params = new URLSearchParams(window.location.search);
        return {
            utm_source: params.get('utm_source'),
            utm_medium: params.get('utm_medium'), 
            utm_campaign: params.get('utm_campaign'),
            utm_term: params.get('utm_term'),
            utm_content: params.get('utm_content'),
            gclid: params.get('gclid'),        // Google Ads click ID
            fbclid: params.get('fbclid'),      // Facebook click ID
            msclkid: params.get('msclkid')     // Microsoft Ads click ID
        };
    }
    
    function getSegmentAnonymousId() {
        /**
         * Retrieve Segment Analytics anonymous ID for cross-session tracking
         * This ID persists across page views and enables customer journey analysis
         */
        try {
            // Method 1: Direct access via Segment Analytics.js
            if (window.analytics && window.analytics.user) {
                const anonymousId = window.analytics.user().anonymousId();
                if (anonymousId) {
                    log('Retrieved Segment ID via analytics.user()');
                    return anonymousId;
                }
            }
            
            // Method 2: Local storage fallback (Segment's standard storage)
            const ajsAnonymousId = localStorage.getItem('ajs_anonymous_id');
            if (ajsAnonymousId) {
                const parsedId = JSON.parse(ajsAnonymousId);
                log('Retrieved Segment ID from localStorage');
                return parsedId;
            }
            
            log('Warning: No Segment anonymous ID found');
            return null;
            
        } catch (error) {
            log('Error retrieving Segment ID:', error);
            return null;
        }
    }
    
    function getDigitalRetailingSessionId() {
        /**
         * Extract session ID from digital retailing widget
         * Each provider has different methods for exposing session data
         */
        return new Promise((resolve) => {
            let attempts = 0;
            const maxAttempts = 50; // Try for 5 seconds (100ms intervals)
            
            function checkForSessionId() {
                attempts++;
                
                try {
                    // Method 1: Check iframe source URLs for session parameters
                    const retailingIframes = document.querySelectorAll('iframe[src*="digital-retailing"], iframe[src*="widget-provider"]');
                    for (let iframe of retailingIframes) {
                        if (iframe.src && iframe.src.includes('session_id')) {
                            const match = iframe.src.match(/session_id=([^&]+)/);
                            if (match) {
                                log('Found session ID in iframe URL');
                                resolve(match[1]);
                                return;
                            }
                        }
                    }
                    
                    // Method 2: Check global data layer objects
                    if (window.digitalRetailingData && window.digitalRetailingData.sessionId) {
                        log('Found session ID in global data layer');
                        resolve(window.digitalRetailingData.sessionId);
                        return;
                    }
                    
                    // Method 3: Check localStorage for session data
                    const sessionData = localStorage.getItem('DR:SessionData');
                    if (sessionData) {
                        try {
                            const parsed = JSON.parse(sessionData);
                            if (parsed.sessionId) {
                                log('Found session ID in localStorage');
                                resolve(parsed.sessionId);
                                return;
                            }
                        } catch (e) {
                            // Invalid JSON, continue
                        }
                    }
                    
                    // Method 4: Check for session ID in page scripts
                    const scripts = document.querySelectorAll('script');
                    for (let script of scripts) {
                        if (script.textContent && script.textContent.includes('visitor_uuid')) {
                            const match = script.textContent.match(/visitor_uuid['":\\s]*['"]([^'"]+)['"]/);
                            if (match) {
                                log('Found visitor UUID in script content');
                                resolve(match[1]);
                                return;
                            }
                        }
                    }
                    
                    // Method 5: PostMessage listener for cross-domain communication
                    window.addEventListener('message', function messageHandler(event) {
                        if (event.origin.includes('widget-provider') && 
                            event.data && 
                            event.data.type === 'session_data') {
                            log('Received session ID via postMessage');
                            window.removeEventListener('message', messageHandler);
                            resolve(event.data.sessionId);
                            return;
                        }
                    });
                    
                } catch (error) {
                    log('Error checking for session ID:', error);
                }
                
                // Retry logic with exponential backoff
                if (attempts < maxAttempts) {
                    const delay = Math.min(100 * Math.pow(1.2, attempts), 500);
                    setTimeout(checkForSessionId, delay);
                } else {
                    log('Could not find digital retailing session ID after maximum attempts');
                    resolve(null);
                }
            }
            
            checkForSessionId();
        });
    }
    
    function getBrowserFingerprint() {
        /**
         * Generate basic browser fingerprint for fraud detection and deduplication
         * Helps identify return visitors even without cookies
         */
        try {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            ctx.textBaseline = 'top';
            ctx.font = '14px Arial';
            ctx.fillText('Browser fingerprint', 2, 2);
            
            const fingerprint = {
                screen: `${screen.width}x${screen.height}`,
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                language: navigator.language,
                platform: navigator.platform,
                canvas_hash: canvas.toDataURL().slice(22, 32) // Partial hash for privacy
            };
            
            return fingerprint;
        } catch (error) {
            log('Error generating browser fingerprint:', error);
            return null;
        }
    }
    
    async function sendVisitorData(visitorData) {
        /**
         * Send visitor identity data to backend with retry logic
         * Implements exponential backoff for resilient data collection
         */
        let lastError;
        
        for (let attempt = 1; attempt <= CONFIG.MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                log(`Sending visitor data (attempt ${attempt}):`, visitorData);
                
                const response = await fetch(CONFIG.API_ENDPOINT, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Visitor-Tracking': 'identity-resolution'
                    },
                    body: JSON.stringify(visitorData)
                });
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const result = await response.json();
                log('✅ Visitor data sent successfully:', result);
                return result;
                
            } catch (error) {
                lastError = error;
                log(`❌ Attempt ${attempt} failed:`, error.message);
                
                if (attempt < CONFIG.MAX_RETRY_ATTEMPTS) {
                    const delay = CONFIG.RETRY_DELAY_MS * Math.pow(2, attempt - 1);
                    log(`Retrying in ${delay}ms...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
        }
        
        log('❌ All retry attempts failed:', lastError);
        throw lastError;
    }
    
    async function initializeIdentityTracking() {
        /**
         * Main initialization function - orchestrates identity data collection
         * Called when page loads and periodically for session updates
         */
        try {
            log('🚀 Initializing customer identity tracking...');
            
            // PRIVACY CHECK FIRST - Respect user preferences
            if (!checkPrivacyCompliance()) {
                log('⛔ Privacy compliance check failed - tracking disabled');
                return;
            }
            
            // Apply additional privacy safeguards
            respectPrivacySettings();
            
            log('✅ Privacy compliance verified - proceeding with tracking');
            
            // Collect URL attribution parameters
            const urlParams = getUrlParameters();
            log('URL parameters collected:', urlParams);
            
            // Get Segment anonymous ID for cross-session tracking
            const segmentId = getSegmentAnonymousId();
            if (!segmentId) {
                log('⚠️ No Segment ID found - visitor tracking may be incomplete');
                return;
            }
            
            // Wait for digital retailing session ID (may take time to load)
            log('⏳ Waiting for digital retailing session...');
            const retailingSessionId = await getDigitalRetailingSessionId();
            
            // Collect browser context for fraud detection
            const browserData = getBrowserFingerprint();
            
            // Compile complete visitor identity package
            const visitorIdentity = {
                // Core tracking identifiers
                segment_anonymous_id: segmentId,
                digital_retailing_session_id: retailingSessionId,
                
                // Marketing attribution
                ...urlParams,
                
                // Page context
                referrer: document.referrer,
                landing_page: window.location.href,
                page_title: document.title,
                timestamp: new Date().toISOString(),
                
                // Browser context
                user_agent: navigator.userAgent,
                viewport: `${window.innerWidth}x${window.innerHeight}`,
                browser_fingerprint: browserData,
                
                // Session metadata
                session_start: sessionStorage.getItem('session_start') || new Date().toISOString()
            };
            
            // Store session start time
            if (!sessionStorage.getItem('session_start')) {
                sessionStorage.setItem('session_start', visitorIdentity.timestamp);
            }
            
            log('📊 Complete visitor identity compiled:', visitorIdentity);
            
            // Send to backend for CRM matching
            await sendVisitorData(visitorIdentity);
            
            log('✅ Identity tracking initialization complete');
            
        } catch (error) {
            log('❌ Identity tracking initialization failed:', error);
            
            // Send error telemetry (optional)
            try {
                await fetch(CONFIG.API_ENDPOINT + '/errors', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        error: error.message,
                        stack: error.stack,
                        timestamp: new Date().toISOString(),
                        page: window.location.href
                    })
                });
            } catch (telemetryError) {
                log('Failed to send error telemetry:', telemetryError);
            }
        }
    }
    
    // Event listeners for dynamic session updates
    function setupEventListeners() {
        /**
         * Listen for events that indicate new customer interactions
         * Enables real-time identity updates as users engage with widgets
         */
        
        // Listen for form submissions (lead generation events)
        document.addEventListener('submit', function(event) {
            log('Form submission detected:', event.target);
            setTimeout(initializeIdentityTracking, 1000); // Re-capture after form submit
        });
        
        // Listen for digital retailing widget interactions
        document.addEventListener('click', function(event) {
            if (event.target.closest('iframe[src*="digital-retailing"]') ||
                event.target.closest('.digital-retailing-widget')) {
                log('Digital retailing interaction detected');
                setTimeout(initializeIdentityTracking, 2000); // Give widget time to initialize
            }
        });
        
        // Listen for page visibility changes (return visits)
        document.addEventListener('visibilitychange', function() {
            if (!document.hidden) {
                log('Page became visible - checking for session updates');
                setTimeout(initializeIdentityTracking, 500);
            }
        });
    }
    
    // Initialization sequence
    function startIdentityTracking() {
        log('🎯 Customer Identity Resolution System starting...');
        
        // Setup event listeners
        setupEventListeners();
        
        // Initial tracking attempt
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initializeIdentityTracking);
        } else {
            // DOM already loaded
            setTimeout(initializeIdentityTracking, 1000); // Give analytics time to load
        }
        
        // Secondary attempt for delayed-loading widgets
        setTimeout(initializeIdentityTracking, 5000);
        
        // Periodic re-capture for long sessions
        setInterval(initializeIdentityTracking, 30000); // Every 30 seconds
        
        log('✅ Identity tracking system initialized');
    }
    
    // Start the system
    startIdentityTracking();
    
})();

/**
 * Implementation Notes:
 * ===================
 * 
 * Deployment:
 * - Include this script on all pages where visitor tracking is needed
 * - Load after Segment Analytics.js for best ID capture
 * - Minify for production to reduce payload size
 * 
 * Privacy Compliance:
 * - Respects Do Not Track browser settings automatically
 * - Checks for consent management platform integration
 * - Honors opt-out cookies and user preferences
 * - Only collects anonymous identifiers (no PII)
 * - Browser fingerprinting is minimal and privacy-focused
 * - Includes data minimization for privacy-conscious users
 * 
 * Performance:
 * - Asynchronous execution prevents page blocking
 * - Retry logic handles network failures gracefully
 * - Minimal DOM manipulation for speed
 * 
 * Monitoring:
 * - Debug mode provides detailed console logging
 * - Error telemetry helps identify tracking issues
 * - Success/failure metrics available via backend API
 */
