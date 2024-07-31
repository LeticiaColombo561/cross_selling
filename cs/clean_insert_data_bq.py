# ## Groceries Dataset
groceries_dataset = """
DECLARE start_date DATE DEFAULT DATE_ADD(current_date, interval -60 day); 
DECLARE end_date DATE DEFAULT DATE_ADD(current_date, interval -1 day); 

SELECT distinct o.order_id
      , o.country.country_name
      , o.country.country_id
      , o.restaurant.id as partner_id
      , LPAD(p.barcode, 14, '0') as gtin
FROM `peya-bi-tools-pro.il_core.fact_orders` as o , UNNEST (details) as d
LEFT JOIN 
    (
       SELECT p.restaurant_id, 
              p.date_id, 
              p.business_type,
              p.business_name,
              max(is_darkstore) as is_darkstore, 
              max(business_category_id) as business_category_id
       FROM `peya-bi-tools-pro.il_core.dim_historical_partners` p
       INNER JOIN (SELECT restaurant_id, 
                         MAX(date_id) AS max_date,               
                 FROM `peya-bi-tools-pro.il_core.dim_historical_partners`
                 WHERE yyyymmdd >= start_date and  yyyymmdd < end_date
                 GROUP BY 1                 
                 ) m ON m.restaurant_id = p.restaurant_id
                    AND m.max_date = p.date_id
       WHERE yyyymmdd >= start_date and  yyyymmdd < end_date
       GROUP BY 1,2,3,4
       order by 1,2
     ) as h on h.restaurant_id=o.restaurant.id
LEFT JOIN 
    ( SELECT id
        , CASE WHEN gtin is not null THEN REGEXP_EXTRACT(gtin, r'([\d]*\d{7})')
               ELSE REGEXP_REPLACE(integration_code, '[^-0-9a-zA-z]+','') END AS barcode 
      FROM `peya-bi-tools-pro.il_core.dim_product` 
    ) as p on d.product.product_id = p.id 
--LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` as p on d.product.product_id=p.id
LEFT JOIN `peya-data-origins-pro.cl_catalogue.product` as c on LPAD(p.barcode, 14, '0') = LPAD(c.gtin, 14, '0')
LEFT JOIN `peya-bi-tools-pro.il_core.dim_business_category` as bc ON bc.business_category_id=h.business_category_id 

WHERE o.registered_date >= start_date
    and o.registered_date < end_date
    and o.business_type_id=2
    and c.id is not null
   -- and (bc.business_category_name ='Supermercados' or bc.business_category_name is null)
    and o.country.country_name in (
                                   "Uruguay",
                                   "Chile",
                                   "Argentina",
                                   "Perú",
                                   "Venezuela",
                                   "Panamá",
                                   "Ecuador",
                                   "Paraguay",
                                   "Costa Rica",
                                   "Bolivia",
                                   "República Dominicana",
                                   "El Salvador",
                                   "Nicaragua",
                                   "Guatemala",
                                   "Honduras"
                                   )


 """


def delete_historical_recommendation_today(project_id, data_set, t):
    """
    delete today table of recommendations if exist
    """
    delete_historical_table = f"""
    DELETE FROM `{project_id}.{data_set}.recommendation_by_country_historical`
    WHERE audit_date=date('{t}')
    """
    return delete_historical_table


def save_recommendation_historical(project_id, data_set, t):
    """
    update recommendations table
    """
    save_recommendation_hist = f"""
    INSERT `{project_id}.{data_set}.recommendation_by_country_historical`
    SELECT *, date('{t}') as audit_date
    FROM `{project_id}.{data_set}.recommendation_by_country`
    """
    return save_recommendation_hist


def save_ranking_top_products(project_id, data_set, from_date, to_date):
    """
    update ranking table
    """
    save_ranking_products = f"""
    DECLARE start_date DATE DEFAULT DATE_ADD(current_date(), interval {from_date} day); 
    DECLARE end_date DATE DEFAULT DATE_ADD(current_date(), interval {to_date} day); 

    CREATE OR REPLACE TABLE `{project_id}.{data_set}.ranking_products_by_partner`
    AS

    SELECT *
    FROM (
        SELECT * except(items_sold)
            , row_number() over(partition by country_id, partner_id order by items_sold desc) as rank

        FROM(
        SELECT  o.country.country_id
            , o.restaurant.id as partner_id
            , LPAD(p.barcode, 14, '0') as gtin
            , sum(d.quantity) as items_sold

        FROM `peya-bi-tools-pro.il_core.fact_orders` as o , UNNEST (details) as d
        LEFT JOIN 
            (
            SELECT p.restaurant_id, 
                    p.date_id, 
                    p.business_type,
                    p.business_name,
                    max(is_darkstore) as is_darkstore, 
                    max(business_category_id) as business_category_id
            FROM `peya-bi-tools-pro.il_core.dim_historical_partners` p
            INNER JOIN (SELECT restaurant_id, 
                                MAX(date_id) AS max_date,               
                        FROM `peya-bi-tools-pro.il_core.dim_historical_partners`
                        WHERE yyyymmdd >= start_date and  yyyymmdd < end_date
                        GROUP BY 1                 
                        ) m ON m.restaurant_id = p.restaurant_id
                            AND m.max_date = p.date_id
            WHERE yyyymmdd >= start_date and  yyyymmdd < end_date
            GROUP BY 1,2,3,4
            order by 1,2
            ) as h on h.restaurant_id=o.restaurant.id
        LEFT JOIN 
            ( SELECT id
                , CASE WHEN gtin is not null THEN REGEXP_EXTRACT(gtin, r'([\d]*\d{7})')
                    ELSE REGEXP_REPLACE(integration_code, '[^-0-9a-zA-z]+','') END AS barcode 
            FROM `peya-bi-tools-pro.il_core.dim_product` 
            ) as p on d.product.product_id = p.id 
        LEFT JOIN `peya-data-origins-pro.cl_catalogue.product` as c on LPAD(p.barcode, 14, '0') = LPAD(c.gtin, 14, '0')
        LEFT JOIN `peya-bi-tools-pro.il_core.dim_business_category` as bc ON bc.business_category_id=h.business_category_id 

        WHERE o.registered_date >= start_date
            and o.registered_date < end_date
            and o.business_type_id=2
            and c.id is not null
            and o.confirmed_order = 1
        -- and (bc.business_category_name ='Supermercados' or bc.business_category_name is null)
            and o.country.country_name in (
                                           "Uruguay",
                                           "Chile",
                                           "Argentina",
                                           "Perú",
                                           "Venezuela",
                                           "Panamá",
                                           "Ecuador",
                                           "Paraguay",
                                           "Costa Rica",
                                           "Bolivia",
                                           "República Dominicana",
                                           "El Salvador",
                                           "Nicaragua",
                                           "Guatemala",
                                           "Honduras"
                                        )
        GROUP BY 1,2,3
        )
        WHERE items_sold > 0
        ORDER BY country_id, partner_id, rank asc
    )
    WHERE rank <= 30
    ORDER BY partner_id, rank asc
    """
    return save_ranking_products


def save_all_recommendations_mba(project_id, data_set, metric):
    """
    save all recommendations struct
    """
    save_recommendations_mba = f"""
    create or replace table `{project_id}.{data_set}.all_recommendations_mba` as 
     select  cast(country_id as string) country_id,
            'mba' as criteria,
            'antecedent' as type,
             cast(antecedents as string) as code,
             ARRAY_AGG(
                STRUCT(
                  consequents as code,
                  'lift' as metric,
                  cast(lift as string) as metric_value          
                )          
                ) as recommendation,  
     from (select country_id, 
                  antecedents, 
                  consequents,
                  max({metric}) as {metric}, 
           from `{project_id}.{data_set}.recommendation_by_country`
           group by 1,2,3
           )
     group by 1,2,3,4

    """
    return save_recommendations_mba


def export_sns_mba(project_id, data_set):
    """
    update recommendations table
    """

    export_sns_table_mba = f"""
    SELECT *
    FROM `{project_id}.{data_set}.all_recommendations_mba`
    """
    return export_sns_table_mba


def save_all_recommendations_ranking (project_id, data_set):
    """
    save all recommendations struct
    """
    save_recommendations_ranking =  f"""
    create or replace table `{project_id}.{data_set}.all_recommendations_ranking` as 
    select  cast(country_id as string) country_id,
           'ranking' as criteria,
           'partner_id' as type,
            cast(partner_id as string) as code,
            ARRAY_AGG(
               STRUCT(
                 gtin as code,
                 'rank' as metric,
                 cast(rank as string) as metric_value          
               )          
        ) as recommendation,
    from `{project_id}.{data_set}.ranking_products_by_partner`
    group by 1,2,3,4
    """
    return save_recommendations_ranking


def export_sns_ranking(project_id, data_set):
    """
    update recommendations table
    """

    export_sns_table_ranking = f"""
    SELECT *
    FROM `{project_id}.{data_set}.all_recommendations_ranking`
    """
    return export_sns_table_ranking