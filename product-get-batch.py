"""
This lambda returns all product related data
in batch
"""
import environment
import re
from distutils.util import strtobool
import json
import ast
import boto3 
from helper import Helper

ENVIRONMENT = environment.ENVIRONMENT
RDS_CLIENT = boto3.client('rds-data')

if ENVIRONMENT == 'staging':
    CLUSTER_ARN = ''
    BUCKET_URL = 'https://d3ckjemso196la.cloudfront.net/product_assets/thumbnail/'
if ENVIRONMENT == 'production':
    CLUSTER_ARN = ''
    BUCKET_URL = 'https://d48f7equ64qjl.cloudfront.net/product_assets/thumbnail/'

SECRET_ARN = ''

BASE_TABLE = 'product'

KEYWORDS = {
    '__exact': '=',
    '__in': ' IN ',
    '__not': ' IS NOT ',
    '__isnull': ' IS ',
    '__is': ' IS ',
    '__notexact': ' != ',
    '__greaterthanrequals': '>=',
    '__lessthanrequals': '<=',
    '__like': ' LIKE ',
    '__contains': ' @> '
}

NESTED_QUERY_TABLE = {
    'arrangement_data': """ (select entity_arrangement_order.sequence_id, 
    entity_arrangement_order.entity_id, folder_arrangement_order.folder_id, 
    folder_arrangement_order.folder_name, entity_arrangement_order.parent_folder_id,
    entity_arrangement_order.ordering_number as entity_order,
    (CASE WHEN parent_folder_id is NULL
    THEN entity_arrangement_order.ordering_number
    ELSE folder_arrangement_order.ordering_number END) AS library_order
    from entity_arrangement_order left outer join folder_arrangement_order
    on folder_arrangement_order.folder_id = entity_arrangement_order.parent_folder_id) as arrangement_data """,
    'collab_products': """ (select collaboration_id,trim(both '"' from (scene_assets->'asset_name')::text)::int as 
    asset_id from (select collaboration_id, json_array_elements(design->'data'->'assets') 
    as scene_assets from collaboration_design) as collab_items) as collab_products """,
    'scene_products': """(select scene_id,trim(both '"' from (scene_assets->'asset_name')::text)::int as 
    asset_id from (select scene.id as scene_id, json_array_elements(design->'design'->'assets') 
    as scene_assets from scene) as scene_items) as scene_products""",
    'has_access_to' : """(SELECT product_id, string_agg('"'||customer_username||'"', ', ') AS has_access, true as is_shared
    FROM   shared_products
    WHERE is_hidden is not True 
    GROUP  BY product_id) as has_access_to"""
    
}

"""
Dependant table format:
table_name: [join type, join_attributes]
"""
DEPENDANT_TABLES = {
    'user_profile': ['left outer', 'username' , 'product.customer_username', ''],
    'product_information': ['left outer', 'product_id' , 'product.id', ''],
    'subscription_preset': ['inner', 'id', 'user_profile.subscription_preset_id', ''],
    'shared_products': ['left outer', 'product_id', 'product.id', ''],
    'arrangement_data': ['left outer','entity_id', 'product.id', 'sequence_id'],
    'has_access_to' : ['left outer', 'product_id', 'product.id', ''],
    'category' : ['left outer', 'name', 'product.category', ''],
    'collab_products': ['left outer', 'asset_id' , 'product.id', ''],
    'scene_products': ['left outer', 'asset_id' , 'product.id', ''],
    'product_user_assets' : ['left outer', 'product_id', 'product.id', 'asset_username'],
    'product_company_assets' : ['left outer', 'product_id', 'product.id', 'asset_company'],
    'project_products': ['LEFT OUTER', 'product_id', 'product.id', '']
}

CONVERSIONS = {
    'date_conversion': "TO_CHAR(%s,\'DD Month YYYY\')",
    'json_conversion': "%s::jsonb",
    'inverse': "not %s"
}

FILTER_CONVERSION = {
    'date_conversion': "TO_CHAR(%s,\'DD Month YYYY\')",
    'json_conversion': "%s::jsonb"
}

"""
Attribute table format:
[name in table, name passed from frontend, data type, main table, dependant tables, 
prefix, condition for attribute conversion (in select clause), condition for filter conversion (in where clause)]
"""
ATTRIBUTES = [
    ['id', 'id', 'int', BASE_TABLE , [], '', '', ''],
    ['name', 'name', 'str', BASE_TABLE, [], '', '', ''],
    ['brand_id', 'brand_id', 'str', BASE_TABLE, [], '', '', ''],
    ['category', 'category', 'str', BASE_TABLE, [], '', '', ''],
    ['color_name', 'color_name', 'str', BASE_TABLE, [], '', '', ''],
    ['materials', 'materials', 'json', BASE_TABLE, [], '', 'json_conversion', ''],
    ['style_category', 'style_category', 'str', BASE_TABLE, [], '', '', ''],
    ['gtin', 'gtin', 'str', BASE_TABLE, [], '', '', ''],
    ['tags', 'tags', 'json', BASE_TABLE, [], '', 'json_conversion', ''],
    ['customer_username', 'customer_username', 'str', BASE_TABLE, [], '', '', ''],
    ['height', 'height', 'int', BASE_TABLE, [], '', '', ''],
    ['width', 'width', 'int', BASE_TABLE, [], '', '', ''],
    ['depth', 'depth', 'int', BASE_TABLE, [], '', '', ''],
    ['model_status', 'model_status', 'int_arr', BASE_TABLE, [], '', '', ''],
    ['scans', 'scans', 'bool', BASE_TABLE, [], '', '', ''],
    ['is_hidden', 'is_hidden', 'bool', BASE_TABLE, [], '', '', ''],
    ['thumbnail', 'thumbnail', 'str', BASE_TABLE, [], BUCKET_URL, '', ''],
    ['assigned_artist', 'assigned_artist', 'str', BASE_TABLE, [], '', '', ''],
    ['last_modified', 'last_modified', 'str', BASE_TABLE, [], '', 'date_conversion', 'date_conversion'],
    ['created_on', 'created_on', 'str', BASE_TABLE, [], '', 'date_conversion', 'date_conversion'],
    ['last_modified', 'last_modified_stamp', 'str', BASE_TABLE, [], '', '', ''],
    ['model_info', 'dimensions', 'json', BASE_TABLE, [], '', 'json_conversion', ''],
    ['variant_of','variant_of','int', BASE_TABLE, [], '', '', ''],
    ['immediate_parent_variant','immediate_parent_variant','int', BASE_TABLE, [], '', '', ''],
    ['company_id','company_id','int', BASE_TABLE, [], '', '', ''],
    ['additional_company_ids','additional_company_ids','str', BASE_TABLE, [], '', '', ''],
    ['variation_type','variation_type','str', BASE_TABLE, [], '', '', ''],
    ['product_model_type', 'product_model_type', 'str', BASE_TABLE, [], '', '', ''],
    ['uploaded_model', 'uploaded_model', 'bool', BASE_TABLE, [], '', '', ''],
    ['need_to_model', 'need_to_model', 'str', BASE_TABLE, [], '', '', ''],
    ['model_type', 'model_type', 'str', BASE_TABLE, [], '', '', ''],
    ['segmented', 'segmented', 'bool', BASE_TABLE, [], '', '', ''],
    ['price','price','float', BASE_TABLE, [], '', '', ''],
    ['is_store_item','is_store_item','int_arr', BASE_TABLE, [], '', '', ''],
    ['group_id', 'group_id', 'str', BASE_TABLE, [], '', '', ''],
    ['artist_pickable', 'hidden_from_artist', 'bool', BASE_TABLE, [], '', 'inverse', ''],
    ['customer_submitted_on', 'customer_submitted_on', 'str', BASE_TABLE, [], '', '', ''],
    ['display_name', 'subscription_display_name', 'str', 'subscription_preset' , ['user_profile', 'subscription_preset'], '', '', ''],
    ['shared_by', 'shared_by', 'str', 'shared_products', ['shared_products'], '', '', ''],
    ['is_hidden', 'shared_hidden', 'bool', 'shared_products', ['shared_products'], '', '', ''],
    ['customer_username', 'shared_username', 'str', 'shared_products', ['shared_products'], '', '', ''],
    ['shared_product_type', 'shared_product_type', 'str', 'shared_products', ['shared_products'], '', '', ''],
    ['company_name', 'company_name', 'str', 'user_profile' , ['user_profile'], '', '', ''],
    ['sequence_id', 'sequence_id', 'int', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['folder_id', 'folder_id', 'int', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['folder_name', 'folder_name', 'str', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['parent_folder_id', 'parent_folder_id', 'int', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['entity_order', 'entity_order', 'int', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['library_order', 'library_order', 'int', 'arrangement_data', ['arrangement_data'], '', '', ''],
    ['has_access', 'has_access_to', 'str', 'has_access_to', ['has_access_to'], '', '', ''],
    ['is_shared', 'is_shared', 'bool', 'has_access_to', ['has_access_to'], '', '', ''],
    ['platform', 'platform', 'str', BASE_TABLE, [], '', '', ''],
    ['created_on', 'created_on_stamp', 'str', BASE_TABLE, [], '', '', ''],
    ['requested_for', 'requested_for', 'str', 'product_information', ['product_information'], '', '', ''],
    ['requested_for_company', 'requested_for_company', 'str', 'product_information', ['product_information'], '', '', ''],
    ['placement_type', 'placement_type', 'str', 'category', ['category'], '', '', ''],
    ['material_type', 'material_type', 'str', 'category', ['category'], '', '', ''],
    ['collaboration_id', 'collaboration_id', 'int', 'collab_products', ['collab_products'], '', '', ''],
    ['scene_id', 'scene_id', 'int', 'scene_products', ['scene_products'], '', '', ''],
    ['render_count', 'user_render_count', 'int', 'product_user_assets', ['product_user_assets'], '', '', ''],
    ['render_count', 'company_render_count', 'int', 'product_company_assets', ['product_company_assets'], '', '', ''],
    ['lifestyle_render_count', 'company_lifestyle_render_count', 'int', 'product_company_assets', ['product_company_assets'], '', '', ''],
    ['customer_username', 'asset_username', 'str', 'product_user_assets', ['product_user_assets'], '', '', ''],
    ['company_id', 'asset_company', 'int', 'product_company_assets', ['product_company_assets'], '', '', ''],
    ['id', 'product_ids', 'int_arr', BASE_TABLE , [], '', '', ''],
    ['project_id', 'project_id', 'int', 'project_products', ['project_products'], '', '', ''],
    ['project_id', 'project_ids', 'int_arr', 'project_products', ['project_products'], '', '', ''],
    ['has_active_project', 'has_active_project', 'bool', BASE_TABLE , [], '', '', ''],
    ['is_ai_model', 'is_ai_model', 'bool', 'product_information', ['product_information'], '', '', ''],
    ['ai_render_count', 'user_ai_render_count', 'int', 'product_user_assets', ['product_user_assets'], '', '', ''],
    ['ai_render_count', 'company_ai_render_count', 'int', 'product_company_assets', ['product_company_assets'], '', '', ''],
]

def get_dimensions(model_info):
    """
    Return model_info data as width, depth and height
    """
    try:
        model_info = json.loads(model_info)
    except:
        pass
    dimensions = {}
    if 'model_info' in model_info:
        model_info = model_info['model_info']
    if 'high' in model_info:
        if 'width' in model_info['high']:
            dimensions['width'] = model_info['high']['width']
        if 'depth' in model_info['high']:
            dimensions['depth'] = model_info['high']['depth']
        if 'height' in model_info['high']:
            dimensions['height'] = model_info['high']['height']
    elif 'low' in model_info:
        if 'width' in model_info['low']:
            dimensions['width'] = model_info['low']['width']
        if 'depth' in model_info['low']:
            dimensions['depth'] = model_info['low']['depth']
        if 'height' in model_info['low']:
            dimensions['height'] = model_info['low']['height']
    else:
        # for backward compatibility of older products
        if 'width' in model_info:
            dimensions['width'] = model_info['width']
        if 'depth' in model_info:
            dimensions['depth'] = model_info['depth']
        if 'height' in model_info:
            dimensions['height'] = model_info['height']
    return dimensions


CUSTOM_JSON_PARSING = {
    'dimensions' : get_dimensions
}

def lambda_handler(event, _):
    """
    Return data according to the filters passed
    """
    print(event)
    post_request_data = event
    
    response = {}
    validation_result, filter_string, filter_tables = run_validation_check(post_request_data)
    if validation_result is None:
        response_data = get_data_in_batch(post_request_data, filter_string, filter_tables)
        if 'compress_response' in post_request_data and post_request_data['compress_response']:
            response = Helper.compress_data(response_data)
        else:
            response = response_data
        
    else:
        response = validation_result
    return response

def run_validation_check(post_request_data):
    """
    Check if validation passes
    """

    if 'required_fields' not in post_request_data:
        return "No required_fields specified", "", []
    else:
        if isinstance(post_request_data['required_fields'], list):
            if len(post_request_data['required_fields']) == 0:
                return "required_fields length should be greater than 1", "", []
            else:
                # Check if required fields has any invalid attribute
                for field in post_request_data['required_fields']:
                    attibute_value = next((attr for attr in ATTRIBUTES if attr[1] == field), None)
                    if attibute_value is None:
                        return f"{field} is not a valid attribute in required_fields.", "", []
        else:
            return "required_fields should be a list", "", []

    if 'filter_string' not in post_request_data:
        return "No filter_string specified", "", []
    elif post_request_data['filter_string'] == "":
        return "filter_string cannot be empty", "", []

    if 'order_by' not in post_request_data:
        return "No order_by specified", "", []
    elif post_request_data['order_by'] == "":
        return "order_by cannot be empty", "", []
    else:
        order_by_attributes = post_request_data['order_by'].split(',')

        for order_attr in order_by_attributes:
            order_attr = order_attr.replace('asc', '').replace('desc', '').replace(' ','')
            attribute_value = next((attr for attr in ATTRIBUTES if attr[1] == order_attr), None)
            if attribute_value is None:
                return f"{order_attr} is not a valid attribute in order_by.", ""

    error_message, filter_string, filter_fields = parse_and_validate_filters_strings(post_request_data['filter_string'])
    
    return error_message, filter_string, filter_fields


def get_data_in_batch(post_request_data, filter_string, filter_tables):
    """
    Construct query according to:
    - required fields
    - attribute filters
    - pagination filters
    """
    required_fields = post_request_data['required_fields']
    order_by = post_request_data['order_by']
    
    query = query_construction(required_fields, filter_string, order_by, filter_tables, post_request_data)
   
    return fetch_data_from_db(query, required_fields)


def query_construction(required_fields, filters, order_by, filter_tables, post_request_data):
    """
    Only add necessary joins when certain fields are required
    """
    attributes = get_attributes(required_fields)
    query_joins = add_joins(required_fields, filter_tables)
    query_filters = filters

    order_by = add_order_by(order_by)

    pagination = ''
    if 'pagination_filters' in post_request_data:
        pagination_filters = post_request_data['pagination_filters']
        pagination = get_pagination_parameters(pagination_filters)

    query_conditions = query_joins + query_filters
    query_ordering_conditions = order_by + pagination

    query = f"select DISTINCT {attributes} from {BASE_TABLE} {query_conditions} {query_ordering_conditions}"

    return query


def add_joins(required_fields, filter_fields):
    """
    Add joins according to required fields 
    passed in payload
    """
    existing_joins = []
    join_condition = ''
    for attr in ATTRIBUTES:
        if (attr[1] in required_fields or attr[1] in filter_fields) and BASE_TABLE not in attr[4]:
            for table in attr[4]:
                if table not in existing_joins:
                    existing_joins.append(table)
                    table_value = table
                    if table in NESTED_QUERY_TABLE:
                        table_value = NESTED_QUERY_TABLE[table]
                    join_condition = f" {join_condition} {DEPENDANT_TABLES[table][0]} join {table_value}\
                    on {table}.{DEPENDANT_TABLES[table][1]} = {DEPENDANT_TABLES[table][2]}"

                    if DEPENDANT_TABLES[table][3] != '' and filter_fields[DEPENDANT_TABLES[table][3]] is not None:
                        join_condition = f"{join_condition} and {filter_fields[DEPENDANT_TABLES[table][3]]}"
    
    return join_condition


def add_order_by(order_by):
    """
    Add order_by condition in query according to
    order by condition passed in payload
    """
    
    
    order_by_attributes = order_by.split(',')
    order_condition = []
    new_order_attribute = ""

    for order_attr in order_by_attributes:
        sort_as = 'desc'
        if 'desc' in order_attr:
            sort_as = 'desc'
        elif 'asc' in order_attr:
            sort_as = 'asc'
        order_attr = order_attr.replace('asc', '').replace('desc', '').replace(' ', '')
        attribute_value = next((attr for attr in ATTRIBUTES if attr[1] == order_attr), None)
        new_order_attribute = f"{attribute_value[3]}.{attribute_value[0]}"
        order_condition.append(f"{new_order_attribute} {sort_as}")
    order_string = ','.join(order_condition)
    order_by_condition = f" order by {order_string} "
    return order_by_condition


def parse_and_validate_filters_strings(filters_string):
    """
    Add filters in where clause of query according to
    filters passed in payload
    """
    error_message = None
    filter_fields = {}
    try:
        # Split conditions according to parentheses in the filter string
        filter_parts = re.findall(r'\([^()]*\)', filters_string)
        if len(filter_parts) > 0:
            for filter_part in filter_parts:
                filter_part = filter_part[1:-1]#remove parentheses
                filter_part = filter_part.replace('&&', '||')
                conditions = filter_part.split('||')
                for condition in conditions:
                    original_condition = condition
                    parts = condition.split('__')
                    attribute = parts[0] #attribute name
                    check_type = parts[1].split('=')[0]## type of keyword passed
                    value = parts[1].split('=')[1]
                    check_type = '__' + check_type
                    ## Check if filter value is present in the attribute array
                    ## Validate filter value and return it if it's valid or not
                    error_message, condition = validate_filter_condition(original_condition)
                    if error_message is None:
                        #Replace attribute identifier with DB attribute name
                        for attribute_data in ATTRIBUTES:
                            if attribute == attribute_data[1]:
                                attribute_value = f"{attribute_data[3]}.{attribute_data[0]}"
                                if attribute not in filter_fields:
                                    filter_fields[attribute] = None
                                if attribute_data[7] != '':
                                    if attribute_data[7] == 'exists_in_array':
                                        attribute_value = FILTER_CONVERSION[attribute_data[7]] % (
                                        attribute_value, value)
                                        condition = condition.replace('__' + parts[1], '')
                                    else:
                                        attribute_value = FILTER_CONVERSION[attribute_data[7]] % (
                                            attribute_value)
                                condition = condition.replace(attribute, attribute_value)
                        
                        if check_type in KEYWORDS:
                            condition = condition.replace(check_type + '=', KEYWORDS[check_type])
                        else: 
                            error_message = f"{check_type} is an invalid operation."
                            return error_message, "", []
                        if check_type == '__in':
                            condition = condition.replace('[', '(').replace(']', ')')
                        elif check_type == '__isnull':
                            condition = condition.replace('true', ' NULL ').replace('false', ' not NULL ')
                            condition = condition.replace('True', ' NULL ').replace('False', ' not NULL ')
                        if check_type == '__exact':
                            if attribute in filter_fields:
                                filter_fields[attribute] = condition

                        filters_string = filters_string.replace(original_condition, condition)
                    else:
                        return error_message, "", []
            filters_string = filters_string.replace('&&', ' and ')
            filters_string = filters_string.replace('||', ' or ')

            return error_message, " where " + filters_string, filter_fields
        else:
            return "Invalid filter string", "", []
    except ValueError as e:
        error_message =  f"Error in filter string parsing. {e}"
        return error_message, "", []


def validate_filter_condition(post_request_data):
    """
    Validate filter condition and convert to valid condition
    if possible
    """
    error_message = None
    condition_split = post_request_data.split('__')
    first_part = condition_split[0]# attribute name
    middle_split = condition_split[1].split('=')#keyword and value
    second_part = middle_split[1]#attribute value
    middle_part = middle_split[0]#keyword value
    value = next((attr for attr in ATTRIBUTES if attr[1] == first_part), None)
    validated_value = ''
    if value is not None:
        if value[2] != 'str':
            second_part = second_part.replace("'", '')
            second_part = second_part.replace('"', '')
        if value[2] == 'int':
            try:
                validated_value = second_part
                if middle_part == 'isnull':
                    validated_value = second_part
                else:
                    validated_value = int(second_part)
            except ValueError:
                validated_value = ''
                error_message = f"{first_part} is not a valid integer."
                return error_message, ""
        elif value[2] == 'int_arr':
            try:
                second_part = ast.literal_eval(second_part)
                res = [int(x) for x in second_part]
                validated_value = res
            except ValueError:
                validated_value = ''
                error_message = f"{first_part} is an invalid list string."
                return error_message, ""
        elif value[2] == 'str':
            try:
                validated_value = second_part
                validated_value = str(second_part)
            except ValueError:
                validated_value = ''
                error_message = f"{first_part} is not a valid string."
                return error_message, ""
        elif value[2] == 'bool':
            try:
                validated_value = second_part
                validated_value = bool(strtobool(str(second_part)))
            except ValueError:
                validated_value = ''
                error_message = f"{first_part} is not a valid boolean."
                return error_message, ""
        
        return error_message, first_part + '__' + middle_part + '=' + str(validated_value)
    else:
        error_message = f"{first_part} filter does not exist."
        return error_message, ""


def fetch_data_from_db(query, required_fields):
    """
    Fetch response from database using try exception
    """
    response_data = []

    # try fetching the data in single query
    try:
        response = execute_query(query, [])
        response_data = generate_response(response['records'], required_fields)
        
        return response_data

    except Exception as e:
        print('Exception Occured ', e)
        count = get_total_rows_to_be_returned(query, [])
        records_per_query = 1000
        offset = 0
        response = []

        while offset <= count: # at this point offset will be the records already fetched
            sql_query = query + ' limit {limit} offset {offset}'.format(limit=records_per_query, offset=offset)
            res = execute_query(sql_query, [])
            response.extend(res['records'])
            offset = offset + records_per_query
        
        response_data.extend(generate_response(response, required_fields))
            
        return response_data


def generate_response(query_response, required_fields):
    """
    Create and return response data
    """
    response_data = {}
    response = []

    for db_record in query_response:
        i = 0
        for attr in ATTRIBUTES:
            if attr[1] in required_fields:
                if 'isNull' not in db_record[i]:
                    if 'longValue' in db_record[i]:
                        response_data[attr[1]] = db_record[i]['longValue']
                    if 'doubleValue' in db_record[i]:
                        response_data[attr[1]] = str(db_record[i]['doubleValue'])
                    if 'stringValue' in db_record[i]:
                        if attr[2] in ['str', 'date', 'decimal']:
                            response_data[attr[1]] = attr[5] + db_record[i]['stringValue']
                        if attr[2] == 'json':
                            if attr[1] in CUSTOM_JSON_PARSING:
                                response_data[attr[1]] = parse_special_jsons(attr[1], db_record[i]['stringValue'])
                            else:
                                if 'data' in json.loads(db_record[i]['stringValue']):
                                    response_data[attr[1]] = json.loads(db_record[i]['stringValue'])['data']
                                elif attr[1] in json.loads(db_record[i]['stringValue']):
                                    response_data[attr[1]] = json.loads(db_record[i]['stringValue'])[attr[1]]
                                else:
                                    response_data[attr[1]] = json.loads(db_record[i]['stringValue'])
                    if 'booleanValue' in db_record[i]:
                        response_data[attr[1]] = db_record[i]['booleanValue']
                else:
                    if attr[2] in ('int', 'float', 'str', 'int_arr'):
                        response_data[attr[1]] = ''
                    elif attr[2] == 'bool':
                        response_data[attr[1]] = False
                    else:
                        response_data[attr[1]] = []

                i = i + 1
        response.append(response_data.copy())

    return response


def get_pagination_parameters(pagination_filters):
    """
    Get limit and offset parameters
    """

    limit = None
    offset = None
    
    if 'limit' in pagination_filters:
        limit = pagination_filters['limit']
    if 'offset' in pagination_filters:
        offset = pagination_filters['offset']
    
    if offset is not None and limit is not None:
        return f" limit {limit} offset {offset} "

    return ''


def get_total_rows_to_be_returned(query, param_set):
    """
    Returns the total # of rows to be returned in the query response
    """
    query_to_get_count = f"select count(id) from ({query}) as subquery"
    
    response = execute_query(query_to_get_count, param_set)
    count = 0
    if 'records' in response:
        count = int(response['records'][0][0]['longValue'])
    return count


def execute_query(query, param_set = []):
    """
    Perform database operation
    """

    print('Query = ', query)
    response = RDS_CLIENT.execute_statement(
                resourceArn = CLUSTER_ARN,
                secretArn = SECRET_ARN,
                database = 'all3d_staging',
                parameters = param_set,
                sql = query)
    
    return response


def parse_special_jsons(attribute_name, data):
    """
    Parse special jsons which need functions to parse
    and return data
    """
    parsed_value = CUSTOM_JSON_PARSING[attribute_name](data) if attribute_name in CUSTOM_JSON_PARSING else None
    
    return parsed_value


def get_attributes(required_fields):
    """
    Get attributes for DB queries according to
    attribute_filters passed
    """

    attributes = []

    i = 0
    for attribute in ATTRIBUTES:
        if attribute[1] in required_fields:
            #BaseTableName.table_attribute
            attribute_value = f"{attribute[3]}.{attribute[0]}"
            #conversion attribute
            if attribute[6] != '':
                attribute_value = CONVERSIONS[attribute[6]] % (attribute_value)
            attributes.append(attribute_value)
            i = i + 1

    attributes_string = ','.join(attributes)
    return attributes_string
