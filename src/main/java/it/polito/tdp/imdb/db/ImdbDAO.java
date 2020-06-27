package it.polito.tdp.imdb.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.imdb.model.Actor;
import it.polito.tdp.imdb.model.Adiacenza;
import it.polito.tdp.imdb.model.Director;
import it.polito.tdp.imdb.model.Movie;

public class ImdbDAO {
	
	public List<Actor> listAllActors(){
		String sql = "SELECT * FROM actors";
		List<Actor> result = new ArrayList<Actor>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor actor = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				
				result.add(actor);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Movie> listAllMovies(){
		String sql = "SELECT * FROM movies";
		List<Movie> result = new ArrayList<Movie>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Movie movie = new Movie(res.getInt("id"), res.getString("name"), 
						res.getInt("year"), res.getDouble("rank"));
				
				result.add(movie);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public List<Director> listAllDirectors(){
		String sql = "SELECT * FROM directors";
		List<Director> result = new ArrayList<Director>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Director director = new Director(res.getInt("id"), res.getString("first_name"), res.getString("last_name"));
				
				result.add(director);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<String> loadGenres() {
		String sql = "SELECT DISTINCT genre " + 
				"FROM movies_genres " + 
				"ORDER BY genre";
		List<String> result = new ArrayList<String>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				result.add(res.getString("genre"));
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
	}
		
}

	public void loadActorsWithGenres(Map<Integer, Actor> idMap, String genre) {
		String sql = "SELECT DISTINCT a.id,a.first_name,a.last_name,a.gender " + 
				"FROM roles r, movies_genres mg,actors a " + 
				"WHERE r.movie_id=mg.movie_id AND a.id=r.actor_id AND mg.genre = ? " + 
				"ORDER BY a.last_name";
		
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genre);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				
				if(!idMap.containsKey(rs.getInt("id"))) {
					Actor actor = new Actor(rs.getInt("id"), rs.getString("first_name"),rs.getString("last_name"),rs.getString("gender"));
					idMap.put(actor.getId(), actor);
				}
			}

			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
		
	}
	
	public List<Adiacenza> getAdiacenza(Map<Integer, Actor> idMap, String genre) {
		String sql = "SELECT r1.actor_id AS a1, r2.actor_id AS a2, COUNT(*) AS c " + 
				"FROM roles r1,roles r2,movies_genres mg " + 
				"WHERE r1.movie_id=r2.movie_id AND r1.movie_id = mg.movie_id AND " + 
				"mg.genre = ? AND r1.actor_id> r2.actor_id " + 
				"GROUP BY r1.actor_id,r2.actor_id";
		List<Adiacenza> result = new ArrayList<Adiacenza>();
		Connection conn = DBConnect.getConnection();

		try {
			
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genre);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				Actor sorgente = idMap.get(rs.getInt("a1"));
				Actor destinazione = idMap.get(rs.getInt("a2"));
				
				if(sorgente != null && destinazione != null) {
					result.add(new Adiacenza(sorgente, destinazione, rs.getInt("c")));
				} else{
					System.out.println("ERRORE IN GET AD");
				}

			}
			conn.close();
		}catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
		
		return result;
	}
	
}
