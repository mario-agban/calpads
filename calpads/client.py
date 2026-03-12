import requests
import logging
import json
import re
import time
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl, urljoin
from collections import deque
from json import JSONDecodeError
from lxml import etree
from .reports_form import ReportsForm, REPORTS_DL_FORMAT
from .extracts_form import ExtractsForm
from .files_upload_form import FilesUploadForm


class CALPADSClient:

    def __init__(self, username, password):
        print("CALPADS CLIENT PATCH 2026-03-12 A")
        self.host = "https://www.calpads.org/"
        self.username = username
        self.password = password
        self.credentials = {
            'Username': self.username,
            'Password': self.password
        }
        self.visit_history = deque(maxlen=10)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        })
        self.session.hooks['response'].append(self._handle_event_hooks)

        self.log = logging.getLogger(__name__)
        log_fmt = f'%(levelname)s: %(asctime)s {self.__class__.__name__}.%(funcName)s: %(message)s'
        logging.basicConfig(format=log_fmt, level=logging.INFO)

        try:
            self.__connection_status = self._login()
        except RecursionError:
            self.log.info("Looks like the provided credentials might be incorrect. Confirm credentials.")
            self.__connection_status = False
        except Exception as e:
            self.log.exception("Login failed with exception: %s", e)
            self.__connection_status = False

    def _login(self):
        """Login method which generally doesn't need to be called except when initializing the client."""
        self.session.get(self.host)
        if not self.visit_history:
            return False

        last = self.visit_history[-1]
        return (
            last.status_code == 200
            and urlsplit(last.url).netloc == "www.calpads.org"
            and not last.url.lower().startswith("https://www.calpads.org/login")
        )

    @property
    def is_connected(self):
        return self.__connection_status

    def get_leas(self):
        response = self.session.get(urljoin(self.host, 'Leas?format=JSON'))
        return safe_json_load(response)

    def get_all_schools(self, lea_code):
        response = self.session.get(urljoin(self.host, f"/SchoolListingAll?lea={lea_code}&format=JSON"))
        return safe_json_load(response)

    def get_submitter_names(self, lea_code):
        response = self.session.get(urljoin(self.host, f"/GetSubmitterNames?leaCdsCode={lea_code}&format=JSON"))
        return safe_json_load(response)

    def get_user_orgs(self, lea_code, email):
        self._select_lea(lea_code)
        response = self.session.get(urljoin(self.host, f"/GetUserOrgs/{email}?format=JSON"))
        if response.status_code == 200:
            return safe_json_load(response)
        return json.loads('{"Data": [],"Total Count": 0}')

    def get_homepage_important_messages(self):
        response = self.session.get(urljoin(self.host, '/HomepageImportantMessages?format=JSON&skip=0&take=5&undefined=0'))
        return safe_json_load(response)

    def get_homepage_anomaly_status(self):
        response = self.session.get(urljoin(self.host, '/HomepageAnomalyStatus?format=JSON'))
        return safe_json_load(response)

    def get_homepage_certification_status(self):
        response = self.session.get(urljoin(self.host, '/HomepageCertificationStatus?format=JSON'))
        return safe_json_load(response)

    def get_homepage_submission_status(self):
        response = self.session.get(urljoin(self.host, '/HomepageSubmissions?format=JSON'))
        return safe_json_load(response)

    def get_homepage_extract_status(self):
        response = self.session.get(urljoin(self.host, '/HomepageNotifications?format=JSON'))
        return safe_json_load(response)

    def get_enrollment_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Enrollment?format=JSON'))
        return safe_json_load(response)

    def get_demographics_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Demographics?format=JSON'))
        return safe_json_load(response)

    def get_address_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Address?format=JSON'))
        return safe_json_load(response)

    def get_elas_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/EnglishLanguageAcquisition?format=JSON'))
        return safe_json_load(response)

    def get_program_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Program?format=JSON'))
        return safe_json_load(response)

    def get_student_course_section_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/StudentCourseSection?format=JSON'))
        return safe_json_load(response)

    def get_cte_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/CareerTechnicalEducation?format=JSON'))
        return safe_json_load(response)

    def get_stas_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/StudentAbsenceSummary?format=JSON'))
        return safe_json_load(response)

    def get_sirs_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/StudentIncidentResult?format=JSON'))
        return safe_json_load(response)

    def get_soff_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Offense?format=JSON'))
        return safe_json_load(response)

    def get_assessment_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/Assessment?format=JSON'))
        return safe_json_load(response)

    def get_sped_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/SPED?format=JSON'))
        return safe_json_load(response)

    def get_serv_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/SERV?format=JSON'))
        return safe_json_load(response)

    def get_swds_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/SWDS?format=JSON'))
        return safe_json_load(response)

    def get_ssrv_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/SSRV?format=JSON'))
        return safe_json_load(response)

    def get_meet_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/MEET?format=JSON'))
        return safe_json_load(response)

    def get_psts_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/PSTS?format=JSON'))
        return safe_json_load(response)

    def get_plan_history(self, ssid):
        response = self.session.get(urljoin(self.host, f'/Student/{ssid}/PLAN?format=JSON'))
        return safe_json_load(response)

    def get_requested_extracts(self, lea_code):
        response = self.session.get(urljoin(self.host, f'/Extract?SelectedLEA={lea_code}&format=JSON'))
        return safe_json_load(response)

    def get_staff_demographics_history(self, seid):
        response = self.session.get(urljoin(self.host, f'/Staff/{seid}/StaffDemographics?format=JSON'))
        return safe_json_load(response)

    def get_staff_assignments_history(self, seid):
        response = self.session.get(urljoin(self.host, f'/Staff/{seid}/StaffAssignments?format=JSON'))
        return safe_json_load(response)

    def get_staff_courses_history(self, seid):
        response = self.session.get(urljoin(self.host, f'/Staff/{seid}/StaffCourses?format=JSON'))
        return safe_json_load(response)

    def request_extract(self, lea_code, extract_name, form_data=None, by_date_range=False,
                        by_as_of_date=False, dry_run=False):
        extract_name = extract_name.upper()
        if not form_data:
            form_data = []

        with self.session as session:
            self._select_lea(lea_code)

            if extract_name == 'SSID':
                session.get('https://www.calpads.org/Extract/SSIDExtract')
            elif extract_name == 'DIRECTCERTIFICATION':
                session.get('https://www.calpads.org/Extract/DirectCertificationExtract')
            elif extract_name == 'REJECTEDRECORDS':
                session.get('https://www.calpads.org/Extract/RejectedRecords')
            elif extract_name == 'CANDIDATELIST':
                session.get('https://www.calpads.org/Extract/CandidateList')
            elif extract_name == 'REPLACEMENTSSID':
                session.get('https://www.calpads.org/Extract/ReplacementSSID')
            elif extract_name == 'SPEDDISCREPANCYEXTRACT':
                session.get('https://www.calpads.org/Extract/SPEDDiscrepancyExtract')
            elif extract_name == 'DSEAEXTRACT':
                session.get('https://www.calpads.org/Extract/DSEAExtract')
            else:
                session.get(f'https://www.calpads.org/Extract/ODSExtract?RecordType={extract_name}')

            root = etree.fromstring(self.visit_history[-1].text, etree.HTMLParser(encoding='utf8'))

            if by_date_range:
                try:
                    if extract_name != 'CENR':
                        chosen_form = root.xpath('//form[contains(@action, "Extract") and contains(@action, "Date")]')[0]
                    else:
                        chosen_form = root.xpath('//form[contains(@action, "Extract") and contains(@action, "DateRange")]')[0]
                except IndexError:
                    self.log.info("There is no By Date Range request option. Falling back to the default form option.")
                    chosen_form = root.xpath('//form[contains(@action, "Extract") and not(contains(@action, "Date"))]')[0]
            elif extract_name == 'CENR' and by_as_of_date:
                chosen_form = root.xpath('//form[contains(@action, "Extract") and contains(@action, "AsofDate")]')[0]
            else:
                chosen_form = root.xpath('//form[contains(@action, "Extract") and not(contains(@action, "Date"))]')[0]

            extracts_form = ExtractsForm(chosen_form)
            if dry_run:
                return extracts_form.get_parsed_form_fields()

            default_filled_fields = extracts_form.prefilled_fields.copy()

            if form_data is not None and dry_run is False:
                keys_in_form_data = {key for key, _ in form_data}
                keys_in_form_data.add('ReportingLEA')
                filtered_filled_fields = [item for item in default_filled_fields if item[0] not in keys_in_form_data]
            else:
                filtered_filled_fields = default_filled_fields

            filtered_filled_fields.extend(form_data + [('ReportingLEA', lea_code)])
            filled_fields = filtered_filled_fields
            filled_fields = extracts_form._filter_text_input_fields(filled_fields)

            if extract_name in ['REJECTEDRECORDS', 'CANDIDATELIST', 'SPEDDISCREPANCYEXTRACT', 'SSID']:
                check_submitter = [field for field in filled_fields if field[0] == 'Submitter' and field[1] is not None]
                if not check_submitter:
                    filled_fields.extend([('Submitter', self._get_submitter_id(lea_code, self.username))])
                check_jobid = [field for field in filled_fields if field[0] == 'JobID' and field[1] is not None]
                if not check_jobid:
                    filled_fields.extend([('JobID', self.get_homepage_submission_status().get('Data')[-1]['JobID'])])

            session.post(urljoin(self.host, chosen_form.attrib['action']), data=filled_fields)
            self.log.info("Attempted to request the extract.")

            success_text = 'Extract request made successfully.  Please check back later for download.'
            request_response = etree.fromstring(self.visit_history[-1].text, parser=etree.HTMLParser(encoding='utf8'))
            try:
                success = (success_text == request_response.xpath('//p')[0].text)
            except IndexError:
                success = False

            return success

    def download_extract(self, lea_code, file_name=None, timeout=600, poll=3000, return_bytes=False):
        if poll < 1:
            poll = 1
        if not file_name:
            file_name = 'data'

        with self.session as session:
            self._select_lea(lea_code)
            time_start = time.time()
            extract_request_id = None

            while (time.time() - time_start) < timeout:
                result = self.get_requested_extracts(lea_code).get('Data')
                if result and result[0]['ExtractStatus'] == 'Complete':
                    extract_request_id = result[0]['ExtractRequestID']
                    self.log.info("Found an extract request ID " + extract_request_id)
                    break
                time.sleep(poll)

            if extract_request_id and not return_bytes:
                with open(file_name, 'wb') as f:
                    for attempt in range(5):
                        try:
                            f.write(self._get_extract_bytes(extract_request_id))
                            return True
                        except Exception:
                            print("Retrying " + extract_request_id)
                            time.sleep(5)
                return False
            elif extract_request_id and return_bytes:
                return self._get_extract_bytes(extract_request_id)
            else:
                self.log.info("Download request timed out. The download might have taken too long.")
                return False

    def upload_file(self, lea_code, file_path=None, form_data=None, dry_run=False):
        if not dry_run:
            assert file_path and form_data, "File Path and Form Data are required inputs."

        with self.session as session:
            self._select_lea(lea_code)
            session.get("https://www.calpads.org/FileSubmission/FileUpload")
            root = etree.fromstring(self.visit_history[-1].text, etree.HTMLParser(encoding='utf8'))
            root_form = root.xpath("//div[@id='fileUpload']//form")[0]
            upload_form = FilesUploadForm(root_form)

            if dry_run:
                return upload_form.get_parsed_form_fields()

            prefilled_form = upload_form.prefilled_fields.copy()
            prefilled_form.extend(form_data)
            prefilled_dict = dict(prefilled_form)
            cleaned_filled_form = {k: v for k, v in prefilled_dict.items() if v != '' and v is not None}

            with open(file_path, 'rb') as f:
                file_input = {'FilesUploaded[0].FileName': f}
                session.post(
                    urljoin(self.visit_history[-1].url, root_form.attrib['action']),
                    files=file_input,
                    data=cleaned_filled_form
                )
                self.log.info("Attempted to upload the file.")

            response = etree.fromstring(self.visit_history[-1].text, etree.HTMLParser(encoding='utf8'))
            return bool(response.xpath('//*[contains(@class, "alert alert-success")]'))

    def post_file(self, lea_code, ignore_rejections=False, get_errors=False,
                  submitter_email=None, timeout=180, poll=30):
        if poll < 10:
            poll = 10
        errors = b''

        with self.session as session:
            self._select_lea(lea_code)
            start_time = time.time()

            while (time.time() - start_time) < timeout:
                get_job_status = self.get_homepage_submission_status().get('Data')[-1]
                if get_job_status['SubmissionStatus'] == 'Ready for Review':
                    if get_job_status['Rejected'] == '0':
                        session.get(f"https://www.calpads.org/FileSubmission/Detail/{get_job_status['JobID']}")
                        if self._post_file_post_action().xpath('//*[contains(@class, "alert alert-success")]'):
                            self.log.info("Successfully posted the file.")
                            return True, errors
                        self.log.info("Attempted and failed to post the file.")
                        return False, errors

                    elif get_job_status['Rejected'] != '0' and ignore_rejections:
                        self.log.info("There were rejections, but ignoring those rejections.")
                        if get_errors:
                            errors = self._get_file_submission_rejections(
                                lea_code,
                                get_job_status['FileTypeCode'] + 'ERR',
                                submitter_email,
                                get_job_status['JobID'],
                                timeout,
                                poll
                            )
                        session.get(f"https://www.calpads.org/FileSubmission/Detail/{get_job_status['JobID']}")
                        if self._post_file_post_action().xpath('//*[contains(@class, "alert alert-success")]'):
                            self.log.info("Successfully posted the file.")
                            return True, errors
                        self.log.info("Attempted and failed to post the file.")
                        return False, errors

                    elif get_job_status['Rejected'] != '0' and not ignore_rejections:
                        if get_errors:
                            errors = self._get_file_submission_rejections(
                                lea_code,
                                get_job_status['FileTypeCode'] + 'ERR',
                                submitter_email,
                                get_job_status['JobID'],
                                timeout,
                                poll
                            )
                        self.log.info("Unable to post the latest job because some records were rejected")
                        return False, errors
                else:
                    time.sleep(poll)

            self.log.info("Unable to post the latest job, timed out.")
            return False, errors

    def _get_file_submission_rejections(self, lea_code, record_type, submitter_email,
                                        job_id, timeout, poll):
        self.log.info("Attempting to fetch the latest submission's rejected records.")
        submitter_id = self._get_submitter_id(lea_code, submitter_email) if submitter_email else None
        submitted_fields = [('LEA', lea_code), ('RecordType', record_type),
                            ('JobID', job_id), ('Submitter', submitter_id),
                            ('School', 'All')]
        success = self.request_extract(lea_code, 'REJECTEDRECORDS', submitted_fields)
        if success:
            self.log.info("Successfully requested the rejected records. Attempting download.")
            return self.download_extract(lea_code, timeout=timeout, poll=poll, return_bytes=True) or b'Failed dowloading extract errors'
        self.log.info("Failed to request the rejected records.")
        return b'Failed requesting extract errors'

    def _get_submitter_id(self, lea_code, submitter_email):
        submitter_names = self.get_submitter_names(lea_code)
        try:
            return [submitter['Value'] for submitter in submitter_names if submitter['Text'] == submitter_email][0]
        except IndexError:
            self.log.debug("Could not find the id for the submitter email; will use the email as is.")
            return submitter_email

    def _post_file_post_action(self):
        root = etree.fromstring(self.visit_history[-1].text, etree.HTMLParser(encoding='utf8'))
        form_root = root.xpath('//form[@action="/FileSubmission/Post"]')[0]
        inputs = FilesUploadForm(form_root).prefilled_fields + [('command', 'Post All')]
        input_dict = dict(inputs)
        self.session.post(urljoin(self.host, '/FileSubmission/Post'), data=input_dict)
        self.log.info("Attempted to post all for this submission job.")
        response_root = etree.fromstring(self.visit_history[-1].text, etree.HTMLParser(encoding='utf8'))
        return response_root

    def _get_extract_bytes(self, extract_request_id):
        self.session.get(urljoin(self.host, f'/Extract/DownloadLink?ExtractRequestID={extract_request_id}'))
        return self.visit_history[-1].content

    def _select_lea(self, lea_code):
        with self.session as session:
            session.get(self.host)
            page_root = etree.fromstring(self.visit_history[-1].text, parser=etree.HTMLParser(encoding='utf8'))
            orgchange_form = page_root.xpath("//form[contains(@action, 'UserOrgChange')]")[0]
            try:
                org_form_val = (
                    orgchange_form
                    .xpath(f"//select/option[contains(text(), '{lea_code}')]")[0]
                    .attrib.get('value')
                )
            except IndexError:
                self.log.info("The provided lea_code, %s, does not appear to exist for you.", lea_code)
                raise Exception("Unable to switch to the provided LEA Code")

            request_token = orgchange_form.xpath("//input[@name='__RequestVerificationToken']")[0].get('value')

            session.post(
                urljoin(self.host, orgchange_form.attrib['action']),
                data={
                    'selectedItem': org_form_val,
                    '__RequestVerificationToken': request_token
                }
            )

    def _get_report_link(self, report_code, is_snapshot=False):
        with self.session as session:
            if is_snapshot:
                session.get('https://www.calpads.org/Report/Snapshot')
            else:
                session.get('https://www.calpads.org/Report/ODS')
            response = self.visit_history[-1]
            if report_code == '8.1eoy3' and is_snapshot:
                return 'https://www.calpads.org/Report/Snapshot/8_1_StudentProfileList_EOY3_'
            root = etree.fromstring(response.text, parser=etree.HTMLParser(encoding='utf8'))
            elements = root.xpath("//*[@class='num-wrap-in']")
            for element in elements:
                if report_code == element.text.lower():
                    return urljoin(self.host, element.xpath('./../../a')[0].attrib['href'])
            self.log.info("Failed to find the provided report code.")

    def _handle_event_hooks(self, r, *args, **kwargs):
        self.log.debug("Response STATUS CODE: %s\nChecking hooks for:\n%s\n", r.status_code, r.url)

        parsed = urlsplit(r.url)
        path = parsed.path
        netloc = parsed.netloc.lower()

 if netloc == "www.calpads.org" and path == "/login" and r.status_code == 200:
    self.log.info("Handling new interim CALPADS login page.")
    self.session.cookies.update(r.cookies.get_dict())

    root = etree.fromstring(r.text, parser=etree.HTMLParser(encoding='utf8'))

    href = None

    # Most specific: anchor containing a descendant <strong> with the label
    matches = root.xpath(
        "//a[@href][.//strong[contains(normalize-space(.), 'Login with Existing CALPADS Username')]]"
    )
    if matches:
        href = matches[0].attrib.get("href")

    # Fallback: any anchor containing that phrase anywhere in descendant text
    if not href:
        matches = root.xpath(
            "//a[@href][contains(normalize-space(string(.)), 'Login with Existing CALPADS Username')]"
        )
        if matches:
            href = matches[0].attrib.get("href")

    # Last-resort debug logging
    if not href:
        self.log.warning("Could not find 'Login with Existing CALPADS Username' link on interim login page.")

        anchors = root.xpath("//a[@href]")
        self.log.warning("Found %s anchors on interim login page.", len(anchors))
        for i, a in enumerate(anchors[:10], start=1):
            anchor_text = " ".join("".join(a.itertext()).split())
            self.log.warning("Anchor %s: href=%r text=%r", i, a.attrib.get("href"), anchor_text)

        self.visit_history.append(r)
        return r

    next_url = urljoin(r.url, href)
    self.log.info("Following interim login link: %s", next_url)
    self.session.get(next_url)
    return r

        # Identity provider login page
        elif path == '/Account/Login' and netloc == 'identity.calpads.org' and r.status_code == 200:
            self.log.info("Handling identity.calpads.org /Account/Login")
            self.session.cookies.update(r.cookies.get_dict())

            init_root = etree.fromstring(r.text, parser=etree.HTMLParser(encoding='utf8'))

            self.credentials['__RequestVerificationToken'] = (
                init_root.xpath("//input[@name='__RequestVerificationToken']")[0].get('value')
            )
            self.credentials['ReturnUrl'] = (
                init_root.xpath("//input[@id='ReturnUrl']")[0].get('value')
            )
            self.credentials['AgreementConfirmed'] = "True"

            self.session.post(r.url, data=self.credentials)
            return r

        # Existing OIDC callback handling
        elif path in ['/connect/authorize/callback', '/connect/authorize'] and r.status_code == 200:
            self.log.info("Handling OpenID callback page.")
            self.session.cookies.update(r.cookies.get_dict())

            login_root = etree.fromstring(r.text, parser=etree.HTMLParser(encoding='utf8'))
            forms = login_root.xpath('//form')
            if not forms:
                self.visit_history.append(r)
                return r

            openid_form_data = {
                input_.attrib.get('name'): input_.attrib.get("value")
                for input_ in login_root.xpath('//input')
                if input_.attrib.get('name') is not None
            }
            action_url = forms[0].attrib.get('action')

            scheme, netloc2, path2, query2, frag2 = urlsplit(action_url)
            if not scheme and not netloc2:
                self.session.post(urljoin(self.host, action_url), data=openid_form_data)
            else:
                self.session.post(action_url, data=openid_form_data)
            return r

        else:
            self.visit_history.append(r)
            return r


def safe_json_load(response):
    try:
        return json.loads(response.content)
    except JSONDecodeError:
        return {}
