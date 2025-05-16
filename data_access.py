from ada_db import SnowflakeSession
import pandas as pd
import logging
logger = logging.getLogger(__name__)
import re
import numpy as np
import json
from datetime import datetime as dt
from textwrap import dedent

class Tables:
    """Analysis service.
    Provides functionality required to create and run analysis.
    """

    def __init__(self, database, client):
        """Create Bordereaux service.
        Args:
            database (AdaDB): AdaDB object for accessing ADA analysis data.
        """
        self.db = database
        self.client = client
        self.con = self.db._get_transaction()

    def get_top_claims(self, current_asat):
        statement = dedent(
            f'''
            WITH filtered_data AS (
            SELECT 
                c.claim_ref,
                c.policy_ref,
                c.major_lob,
                c.data_asat_date,
                c.total_inc,
                c.yoa
            FROM prod_claims_data c
            JOIN prod_meta m ON c.prodmeta_id = m.id
            WHERE m.archived_by IS NULL
            AND m.deprecated = FALSE
            AND c.data_asat_date IN ('{current_asat}')
            )
            SELECT policy_ref, claim_ref, yoa, total_inc, major_lob
            FROM (
                SELECT  
                    policy_ref, 
                    claim_ref,
                    yoa,
                    total_inc, 
                    major_lob,
                    ROW_NUMBER() OVER (PARTITION BY major_lob ORDER BY total_inc DESC) AS rn
                FROM filtered_data
                WHERE data_asat_date = '{current_asat}'
            ) sub
            WHERE rn <= 10
            ORDER BY major_lob, total_inc DESC;
        '''
        )
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        current_incurred = pd.DataFrame(all_rows)
        current_incurred.columns = field_names
        
        return current_incurred
    
    def get_biggest_new_claims(self, current_asat, previous_asat):
        statement = dedent(
            f"""
        WITH filtered_data AS (
        SELECT 
            c.claim_ref,
            c.policy_ref,
            c.major_lob,
            c.data_asat_date,
            c.total_inc,
            c.yoa
        FROM prod_claims_data c
        JOIN prod_meta m ON c.prodmeta_id = m.id
        WHERE m.archived_by IS NULL
        AND m.deprecated = FALSE
        AND c.data_asat_date IN ('{current_asat}', '{previous_asat}')
        ),
        current_snapshot AS (
            SELECT *
            FROM filtered_data
            WHERE data_asat_date = '{current_asat}'
        ),
        previous_snapshot AS (
            SELECT DISTINCT claim_ref, policy_ref, yoa
            FROM filtered_data
            WHERE data_asat_date = '{previous_asat}'
        ),
        new_claims AS (
            SELECT c.*
            FROM current_snapshot c
            LEFT JOIN previous_snapshot p
            ON c.claim_ref = p.claim_ref
            AND c.policy_ref = p.policy_ref
            AND c.yoa = p.yoa
            WHERE p.claim_ref IS NULL
        ),
        ranked_new_claims AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY major_lob ORDER BY total_inc DESC) AS rn
            FROM new_claims
        )
        SELECT 
            policy_ref,
            claim_ref,
            major_lob,
            yoa,
            total_inc
        FROM ranked_new_claims
        WHERE rn <= 10
        ORDER BY major_lob, total_inc DESC;
        """
        )
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        top_new_claims = pd.DataFrame(all_rows)
        top_new_claims.columns = field_names
        return top_new_claims

    def get_biggest_movement(self, current_asat, previous_asat):
        statement = dedent(
            f'''
        WITH filtered_data AS (
            SELECT 
                c.claim_ref,
                c.policy_ref,
                c.major_lob,
                c.data_asat_date,
                c.total_inc,
                c.yoa
            FROM prod_claims_data c
            JOIN prod_meta m ON c.prodmeta_id = m.id
            WHERE m.archived_by IS NULL
            AND m.deprecated = FALSE
            AND c.data_asat_date IN ('{current_asat}', '{previous_asat}')
            ),
        ranked_dates AS (
            SELECT DISTINCT data_asat_date
            FROM filtered_data
            WHERE data_asat_date <= '{current_asat}'
            ORDER BY data_asat_date DESC
            LIMIT 2
        ),
        base_data AS (
            SELECT * 
            FROM filtered_data
            WHERE data_asat_date IN (SELECT data_asat_date FROM ranked_dates)
        ),
        pivoted AS (
            SELECT 
                policy_ref,
                claim_ref,
                yoa,
                major_lob,
                MAX(CASE WHEN data_asat_date = '{current_asat}' THEN total_inc END) AS total_inc_current,
                MAX(CASE WHEN data_asat_date = '{previous_asat}' THEN total_inc END) AS total_inc_previous
            FROM base_data
            GROUP BY policy_ref, claim_ref, yoa, major_lob
        )
        SELECT 
            policy_ref,
            claim_ref,
            yoa,
            major_lob,
            total_inc_current,
            total_inc_previous,
            total_inc_current - total_inc_previous AS inc_movement,
            ABS(total_inc_current - total_inc_previous) AS abs_movement
        FROM pivoted
        WHERE total_inc_current IS NOT NULL AND total_inc_previous IS NOT NULL
        ORDER BY abs_movement DESC
        LIMIT 10;
        '''
        )
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        biggest_difference = pd.DataFrame(all_rows)
        biggest_difference.columns = field_names
        return biggest_difference

    def get_top_ftl_claims(self, asat_date):
        statement = dedent(f'''
        WITH filtered_data AS (
            SELECT 
                c.policy_ref,
                c.claim_ref,
                c.major_lob,
                c.data_asat_date,
                c.total_inc,
                c.yoa
            FROM prod_claims_data c
            JOIN prod_meta m ON c.prodmeta_id = m.id
            WHERE m.archived_by IS NULL
            AND m.deprecated = FALSE
            AND c.data_asat_date IN ('{asat_date}')
            ),
        claim_sums AS (
            SELECT 
                policy_ref,
                claim_ref,
                yoa,
                major_lob,
                SUM(total_inc) AS tot_inc
            FROM prod_claims_data
            WHERE data_asat_date = '{asat_date}'
            GROUP BY claim_ref, policy_ref, yoa, major_lob
        ),
        ranked_claims AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY major_lob ORDER BY tot_inc DESC) AS rn
            FROM claim_sums
        )
        SELECT policy_ref, claim_ref, yoa, major_lob, tot_inc
        FROM ranked_claims
        WHERE rn <= 10
        ORDER BY major_lob, tot_inc DESC;
        ''')
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        biggest_claims = pd.DataFrame(all_rows)
        biggest_claims.columns = field_names
        return biggest_claims
    
    def get_biggest_ftl_movement(self, current_asat, previous_asat):
        statement = dedent(
        f'''
        WITH filtered_data AS (
            SELECT 
                c.claim_ref,
                c.policy_ref,
                c.yoa,
                c.major_lob,
                c.data_asat_date,
                c.total_inc
            FROM prod_claims_data c
            JOIN prod_meta m ON c.prodmeta_id = m.id
            WHERE m.archived_by IS NULL
            AND m.deprecated = FALSE
            AND c.data_asat_date IN ('{current_asat}', '{previous_asat}')
        ),
        claim_totals AS (
            SELECT 
                claim_ref,
                policy_ref,
                yoa,
                major_lob,
                data_asat_date,
                SUM(total_inc) AS total_incurred
            FROM filtered_data
            GROUP BY claim_ref, policy_ref, yoa, major_lob, data_asat_date
        ),
        pivoted AS (
            SELECT 
                claim_ref,
                policy_ref,
                yoa,
                major_lob,
                MAX(CASE WHEN data_asat_date = '{current_asat}' THEN total_incurred END) AS current_total,
                MAX(CASE WHEN data_asat_date = '{previous_asat}' THEN total_incurred END) AS previous_total
            FROM claim_totals
            GROUP BY claim_ref, policy_ref, yoa, major_lob
        ),
        diffs AS (
            SELECT 
                *,
                current_total - previous_total AS movement,
                ABS(current_total - previous_total) AS abs_movement
            FROM pivoted
            WHERE movement IS NOT NULL
        ),
        ranked_diffs AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY major_lob ORDER BY abs_movement DESC) AS rn
            FROM diffs
        )
        SELECT 
            policy_ref,
            claim_ref,
            yoa,
            major_lob,
            previous_total,
            current_total,
            movement,
            abs_movement
        FROM ranked_diffs
        WHERE rn <= 10
        ORDER BY major_lob, abs_movement DESC;
        '''
        )
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        biggest_difference = pd.DataFrame(all_rows)
        biggest_difference.columns = field_names
        return biggest_difference

    def top_new_ftl_claims(self, current_asat, previous_asat):
        statement = dedent(
        f"""
        WITH filtered_data AS (
        SELECT 
            c.claim_ref,
            c.policy_ref,
            c.major_lob,
            c.data_asat_date,
            c.total_inc,
            c.yoa
        FROM prod_claims_data c
        JOIN prod_meta m ON c.prodmeta_id = m.id
        WHERE m.archived_by IS NULL
        AND m.deprecated = FALSE
        AND c.data_asat_date IN ('{current_asat}', '{previous_asat}')
        ),
        aggregated_data AS (
            SELECT 
                claim_ref,
                policy_ref,
                yoa,
                major_lob,
                data_asat_date,
                SUM(total_inc) AS total_inc
            FROM filtered_data
            GROUP BY claim_ref, policy_ref, yoa, major_lob, data_asat_date
        ),
        current_snapshot AS (
            SELECT *
            FROM aggregated_data
            WHERE data_asat_date = '{current_asat}'
        ),
        previous_snapshot AS (
            SELECT claim_ref, policy_ref, yoa
            FROM aggregated_data
            WHERE data_asat_date = '{previous_asat}'
        ),
        new_claims AS (
            SELECT c.*
            FROM current_snapshot c
            LEFT JOIN previous_snapshot p 
                ON c.claim_ref = p.claim_ref
            AND c.policy_ref = p.policy_ref
            AND c.yoa = p.yoa
            WHERE p.claim_ref IS NULL
        ),
        ranked_new_claims AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY major_lob ORDER BY total_inc DESC) AS rn
            FROM new_claims
        )
        SELECT 
            claim_ref,
            policy_ref,
            yoa,
            major_lob,
            total_inc
        FROM ranked_new_claims
        WHERE rn <= 10
        ORDER BY major_lob, total_inc DESC;
        """
        )
        cur = self.con.cursor()
        try:
            cur.execute(statement)
            all_rows = cur.fetchall()
            field_names = [i[0] for i in cur.description]
        finally:
            cur.close()
        top_new_ftl_claims = pd.DataFrame(all_rows)
        top_new_ftl_claims.columns = field_names
        return top_new_ftl_claims


def init(client):
    """Initiates the instance of Bordereaux class
    
    Args: 
        client (str): Client name
    
    Returns:
        bordereaux(Class)

    """
    db = SnowflakeSession(client)
    bordereaux = Tables(db, client)
    return bordereaux
