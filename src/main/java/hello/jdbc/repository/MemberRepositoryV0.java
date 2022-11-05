package hello.jdbc.repository;

import hello.jdbc.connection.DBConnectionUtil;
import hello.jdbc.domain.Member;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.NoSuchElementException;

/**
 * JDBC - DriverManager 사용
 */
@Slf4j
public class MemberRepositoryV0 {

    public Member save(Member member) throws SQLException {
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
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, null);
        }

    }

    public Member findById(String memberId) throws SQLException {
        String sql = "select * from member where member_id = ?";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김
        ResultSet rs = null; //DB로부터 응답 결과를 받음

        try {
            con = DBConnectionUtil.getConnection();
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
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, rs);
        }
    }

    public void update(String memberId, int money) throws SQLException {
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
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, null);
        }
    }

    public void delete(String memberId) throws SQLException {
        String sql = "delete from member where member_id=?";

        Connection con = null; //DB와 애플리케이션을 연결
        PreparedStatement pstmt = null; //DB에 SQL을 넘김

        try {
            con = getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, memberId); //sql 파라미터 바인딩
            pstmt.executeUpdate();//커넥션을 통해 SQL을 데이터베이스에 전달하며 영향받은 row수를 반환
        } catch (SQLException e) {
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, null);
        }
    }

    private void close(Connection con, Statement stmt, ResultSet rs) {

        if (rs != null) {
            try {
                rs.close(); //역순으로 연결 끊기
            } catch (SQLException e) {
                log.error("error", e);
            }
        }

        if (stmt != null) {
            try {
                stmt.close(); //역순으로 연결 끊기
            } catch (SQLException e) {
                log.error("error", e);
            }
        }

        if (con != null) {
            try {
                con.close(); //역순으로 연결 끊기
            } catch (SQLException e) {
                log.error("error", e);
            }
        }

    }

    private Connection getConnection() {
        return DBConnectionUtil.getConnection();
    }
}
