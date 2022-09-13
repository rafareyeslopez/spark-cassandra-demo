import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class Click implements Serializable {

	private static SimpleDateFormat formatTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

	private static final long serialVersionUID = 1L;
	private String clickUuid;
	private Date timestamp;
	private Integer applicationId;
	private Integer campaignId;
	private String country;
	private Integer countryId;
	private Date date;
	private String ip;
	private String isp;
	private String landingPageVersion;
	private String networkClick;
	private Integer networkId;
	private Integer premiumServiceId;
	private String publisherId;
	private String queryString;
	private String requestHeaderInfo;
	private String service;
	private String sub_publisherId;
	private String subpublisherId;

	public Click(String clickUuid, Date timestamp, Integer applicationId, Integer campaignId, String country,
			Integer countryId, Date date, String ip, String isp, String landingPageVersion, String networkClick,
			Integer networkId, Integer premiumServiceId, String publisherId, String queryString,
			String requestHeaderInfo, String service, String sub_publisherId, String subpublisherId) {
		super();
		if (clickUuid != null) {
			this.clickUuid = clickUuid;
		} else {
			this.clickUuid = "";
		}
		this.timestamp = timestamp;
		this.applicationId = applicationId;
		this.campaignId = campaignId;
		this.country = country;
		this.countryId = countryId;
		this.date = date;
		this.ip = ip;
		this.isp = isp;
		this.landingPageVersion = landingPageVersion;
		this.networkClick = networkClick;
		this.networkId = networkId;
		this.premiumServiceId = premiumServiceId;
		this.publisherId = publisherId;
		this.queryString = queryString;
		this.requestHeaderInfo = requestHeaderInfo;
		this.service = service;
		this.sub_publisherId = sub_publisherId;
		this.subpublisherId = subpublisherId;
	}

	@Override
	public String toString() {
		String timestampPrint = "";
		if (timestamp != null) {
			timestampPrint = formatTimestamp.format(timestamp);
		}

		String datePrint = "";
		if (date != null) {
			datePrint = formatDate.format(date);
		}

		String requestHeaderInfoPrint = "";
		if (requestHeaderInfo != null) {
			requestHeaderInfoPrint = URLEncoder.encode(requestHeaderInfo, StandardCharsets.UTF_8);
		}

		String ispPrint = "";
		if (isp != null) {
			ispPrint = isp.replaceAll(",", "-");
		}

		return clickUuid + ", " + timestampPrint + "," + applicationId + ", " + campaignId + ", " + country + ", "
				+ countryId + "," + datePrint + ", " + ip + ", " + ispPrint + ", " + landingPageVersion + ", "
				+ networkClick + ", " + networkId + ", " + premiumServiceId + ", " + publisherId + ", " + queryString
				+ ", " + requestHeaderInfoPrint + ", " + service + ", " + sub_publisherId + ", " + subpublisherId;
	}

}
