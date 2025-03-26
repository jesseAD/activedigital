# Migration Checklist: AWS to Linode

## 1. Pre-Migration Planning
- [ ] Document current server specifications and performance metrics
- [ ] Create a timeline for the migration
- [ ] Schedule maintenance window with team
- [ ] Document current MongoDB connection details
- [ ] Document current Grafana dashboard configurations
- [ ] List all exchange API keys and credentials
- [ ] Note current Linode IP address for API whitelisting
- [ ] Document current Linode server configuration

## 2. Initial Server Setup (Temporary on Your Linode)
### Server Configuration
- [ ] Document current Linode instance specifications
- [ ] Verify sufficient resources for the application
- [ ] Configure firewall rules
- [ ] Set up SSH access
- [ ] Install required system packages:
  - [ ] Python 3.9
  - [ ] Docker
  - [ ] Docker Compose
  - [ ] Git
  - [ ] MongoDB client tools

### Environment Setup
- [x] Create Python virtual environment
- [x] Install project dependencies from requirements.txt
- [x] Set up environment variables:
  - [x] MongoDB connection string
  - [x] Exchange API keys
  - [x] Other configuration parameters
- [x] Configure logging
- [ ] Set up monitoring tools

## 3. Data Migration
### MongoDB Setup
- [x] Create new MongoDB Atlas account (if not already done)
- [x] Set up MongoDB Atlas cluster
- [x] Configure MongoDB Atlas security settings
- [x] Create backup of current MongoDB data
- [x] Verify backup integrity
- [x] Restore data to new MongoDB Atlas cluster
- [x] Verify data consistency between old and new databases
- [x] Update MongoDB connection strings in configuration

### Data Verification
- [ ] Verify all collections are present
- [ ] Check data integrity for timeseries collections
- [ ] Verify snapshot data
- [ ] Test data access patterns
- [ ] Verify historical data retention

## 4. Application Deployment
### Docker Setup
- [ ] Build Docker image
- [ ] Test Docker container locally
- [ ] Set up Docker Compose configuration
- [ ] Configure container networking
- [ ] Set up container health checks
- [ ] Configure container logging

### Application Configuration
- [ ] Update configuration files for new environment
- [ ] Configure rate limiting
- [ ] Set up error handling
- [ ] Configure monitoring endpoints
- [ ] Test application startup and shutdown

## 5. Monitoring and Alerts
### Grafana Setup
- [ ] Install Grafana
- [ ] Configure MongoDB data source
- [ ] Import existing dashboards
- [ ] Verify all dashboard panels
- [ ] Test dashboard refresh rates
- [ ] Configure dashboard alerts

### System Monitoring
- [ ] Set up server monitoring
- [ ] Configure application metrics
- [ ] Set up alerting for:
  - [ ] Server health
  - [ ] Application errors
  - [ ] Data collection issues
  - [ ] Position alerts (threshold: 1000)
  - [ ] Options alerts (threshold: 25)

## 6. Security
### Access Control
- [ ] Set up VPN access
- [ ] Configure firewall rules
- [ ] Set up SSH key authentication
- [ ] Configure MongoDB Atlas IP whitelist
- [ ] Set up secure credential storage

### API Security
- [ ] Transfer exchange API keys securely
- [ ] Update API key permissions
- [ ] Test API connections
- [ ] Set up API key rotation schedule

## 7. Backup and Recovery
### Backup Configuration
- [ ] Set up automated MongoDB backups
- [ ] Configure backup retention policy
- [ ] Test backup restoration
- [ ] Document backup procedures

### Disaster Recovery
- [ ] Create disaster recovery plan
- [ ] Document recovery procedures
- [ ] Test recovery process
- [ ] Set up monitoring for backup failures

## 8. Testing
### Application Testing
- [ ] Test data collection from all exchanges
- [ ] Verify position tracking
- [ ] Test alerting system
- [ ] Verify dashboard updates
- [ ] Test error handling

### Performance Testing
- [ ] Test under normal load
- [ ] Verify parallel processing
- [ ] Check memory usage
- [ ] Monitor CPU utilization
- [ ] Test network performance

## 9. Documentation
### System Documentation
- [ ] Document server setup process
- [ ] Create deployment guide
- [ ] Document configuration changes
- [ ] Create troubleshooting guide
- [ ] Document backup procedures

### User Documentation
- [ ] Update user guides
- [ ] Document new access procedures
- [ ] Create FAQ document
- [ ] Document alert meanings
- [ ] Create support contact list

## 10. Rollback Plan
### Rollback Preparation
- [ ] Keep old server running
- [ ] Maintain old MongoDB connection
- [ ] Document rollback procedures
- [ ] Test rollback process
- [ ] Keep old credentials active

### Post-Migration
- [ ] Monitor system for 24-48 hours
- [ ] Verify all alerts are working
- [ ] Check data consistency
- [ ] Get team sign-off
- [ ] Schedule old server decommissioning

## 11. Post-Migration Tasks
- [ ] Clean up old server
- [ ] Update DNS records if needed
- [ ] Update monitoring configurations
- [ ] Schedule regular maintenance
- [ ] Plan future upgrades

## 12. Server Transfer Process
### Pre-Transfer
- [ ] Get team's new Linode account access
- [ ] Document all current server configurations
- [ ] Create backup of all configuration files
- [ ] Document all environment variables
- [ ] List all installed packages and versions
- [ ] Document current IP whitelist settings

### New Server Setup
- [ ] Set up new Linode server with matching specifications
- [ ] Configure firewall rules
- [ ] Install all required packages
- [ ] Set up environment variables
- [ ] Configure Docker and Docker Compose
- [ ] Update MongoDB Atlas IP whitelist with new server IP

### Data Transfer
- [ ] Verify MongoDB connection from new server
- [ ] Test Grafana connection from new server
- [ ] Verify all exchange API connections
- [ ] Test data collection on new server
- [ ] Verify all monitoring systems

### Switchover
- [ ] Schedule switchover window with team
- [ ] Update DNS records if needed
- [ ] Switch Grafana data source to new server
- [ ] Verify all dashboards are working
- [ ] Test all alerting systems
- [ ] Monitor for any issues

### Cleanup
- [ ] Remove old server from MongoDB whitelist
- [ ] Archive old server configuration
- [ ] Document final server specifications
- [ ] Update all documentation with new server details

## Notes
- Keep this checklist updated as you progress
- Document any issues or deviations from plan
- Get team approval for major changes
- Test thoroughly before each major step
- Keep communication channels open with team
- Maintain both servers until new server is fully verified
- Keep old server configuration for reference 