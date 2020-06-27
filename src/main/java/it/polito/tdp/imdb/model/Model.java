package it.polito.tdp.imdb.model;

import java.util.*;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import it.polito.tdp.imdb.db.ImdbDAO;

public class Model {
	private Graph<Actor, DefaultWeightedEdge> grafo;
	private Map<Integer, Actor> idMap;
	ImdbDAO dao;
	
	public Model() {
		idMap = new HashMap<Integer,Actor>();
		dao = new ImdbDAO();
	}
	
	public List<String> loadGenres(){
		return dao.loadGenres();
	}
	
	public void creaGrafo(String genre) {
		this.grafo = new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
		dao.loadActorsWithGenres(idMap,genre);
		
		//Aggiungere i vertici
		Graphs.addAllVertices(this.grafo, idMap.values());
		
		//Aggiungere gli archi
		
		for(Adiacenza a : dao.getAdiacenza(idMap, genre)) {
			if(a.getPeso()!=0) {
				Graphs.addEdge(this.grafo, a.getA1(), a.getA2(), a.getPeso());
			}
		}

	}
	public List<Actor> visitaAmpiezza (Actor source){
		List<Actor> visita = new ArrayList<>();
		
		BreadthFirstIterator<Actor, DefaultWeightedEdge> bfv = new BreadthFirstIterator<>(grafo,source);
		while (bfv.hasNext()) {
		    visita.add(bfv.next());	
		}
		
		visita.remove(source);
		
		Collections.sort(visita);
		return visita;
	}
	
	public List<Actor> getActors(){
		List<Actor> lista = new ArrayList<>(this.grafo.vertexSet());
		Collections.sort(lista);
		return lista;
	}
	public int nVertici() {
		return this.grafo.vertexSet().size();
	}
	
	public int nArchi() {
		return this.grafo.edgeSet().size();
	}
	
	public static void main(String args[]) {
		Model m = new Model();
		Actor a = new Actor(6762,null,null,null);
		m.creaGrafo("Animation");
		
		System.out.println(m.visitaAmpiezza(a));
		}
}
