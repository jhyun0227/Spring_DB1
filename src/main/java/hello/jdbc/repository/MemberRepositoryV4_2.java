package hello.jdbc.repository;

import hello.jdbc.domain.Member;
import hello.jdbc.repository.ex.MyDbException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import javax.sql.DataSource;
import java.sql.*;
import java.util.NoSuchElementException;

/**
 * SQLExceptionTranslator 추가
 */
@Slf4j
public class MemberRepositoryV4_2 implements MemberRepository {

    private final DataSource dataSource;
    private final SQLExceptionTranslator exTranslator;

    public MemberRepositoryV4_2(DataSource dataSource) {
        this.dataSource = dataSource;
        this.exTranslator = new SQLErrorCodeSQLExceptionTranslator(dataSource);
    }

    @Override
    public Member save(Member member) {
        String sql = "insert into member(member_id, money) values (?, ?)";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김

        try {
            con = getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, member.getMemberId()); //sql 파라미터 바인딩
            pstmt.setInt(2, member.getMoney()); //sql 파라미터 바인딩
            pstmt.executeUpdate(); //커넥션을 통해 SQL을 데이터베이스에 전달하며 영향받은 row수를 반환
            return member;
        } catch (SQLException e) {
            DataAccessException ex = exTranslator.translate("save", sql, e);
            throw ex;
        } finally {
            close(con, pstmt, null);
        }

    }

    @Override
    public Member findById(String memberId) {
        String sql = "select * from member where member_id = ?";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김
        ResultSet rs = null; //DB로부터 응답 결과를 받음

        try {
            con = getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, memberId);

            rs = pstmt.executeQuery(); //데이터 조회시에는 executeQuery
            if (rs.next()) {
                Member member = new Member();
                member.setMemberId(rs.getString("member_id"));
                member.setMoney(rs.getInt("money"));
                return member;
            } else {
                throw new NoSuchElementException("member not found memberId = " + memberId);
            }

        } catch (SQLException e) {
            DataAccessException ex = exTranslator.translate("findById", sql, e);
            throw ex;
        } finally {
            close(con, pstmt, rs);
        }
    }

    @Override
    public void update(String memberId, int money) {
        String sql = "update member set money=? where member_id=?";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김

        try {
            con = getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, money); //sql 파라미터 바인딩
            pstmt.setString(2, memberId); //sql 파라미터 바인딩
            int resultSize = pstmt.executeUpdate();//커넥션을 통해 SQL을 데이터베이스에 전달하며 영향받은 row수를 반환
            log.info("resultSize={}", resultSize);
        } catch (SQLException e) {
            DataAccessException ex = exTranslator.translate("update", sql, e);
            throw ex;
        } finally {
            close(con, pstmt, null);
        }
    }

    @Override
    public void delete(String memberId) {
        String sql = "delete from member where member_id=?";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김

        try {
            con = getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, memberId); //sql 파라미터 바인딩
            pstmt.executeUpdate();//커넥션을 통해 SQL을 데이터베이스에 전달하며 영향받은 row수를 반환
        } catch (SQLException e) {
            DataAccessException ex = exTranslator.translate("delete", sql, e);
            throw ex;
        } finally {
            close(con, pstmt, null);
        }
    }

    private void close(Connection con, Statement stmt, ResultSet rs) {
        JdbcUtils.closeResultSet(rs);
        JdbcUtils.closeStatement(stmt);
        //주의 트랜잭션 동기화를 사용하려면 DataSourceUtils를 사용해야 한다.
        DataSourceUtils.releaseConnection(con, dataSource);

    }

    private Connection getConnection() throws SQLException {
        //주의! 트랜잭션 동기화를 사용하려면 DataSourceUtils를 사용해야 한다.
        Connection con = DataSourceUtils.getConnection(dataSource);
        log.info("get connection={}, class={}", con, con.getClass());
        return con;
    }
}
