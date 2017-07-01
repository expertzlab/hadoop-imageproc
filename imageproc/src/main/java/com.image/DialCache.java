package com.image;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Dial's queue for storing 16-bit unsigned affinity values tied to 32-bit signed spel indices.
 * Internally keeps a secondary int, int dictionary to make fetching/updating the affinity of spels O(1)
 * @author Daniel
 *
 */
public class DialCache 
{
    //Highest possible value for the dial cache (here: maximum 16-bit unsigned value)
	public static final int MaxIndex = 65535;
	
	private ArrayList<ArrayList<Integer>> m_arr;
	private HashMap<Integer, Integer> m_pointerArray;
	
	private int m_largestAffinity;	
	public int m_size;
	
	public DialCache()
	{
		m_arr = new ArrayList<ArrayList<Integer>>(MaxIndex + 1);
		for(int i = 0; i <= MaxIndex; i++)
			m_arr.add(new ArrayList<Integer>());
		
		m_pointerArray = new HashMap<Integer, Integer>();		
		m_largestAffinity = 0;		
		m_size = 0;
	}
	
	/**
	 * Adds a spel with its affinity to the cache
	 * @param spelIndex Integer indicating the spel's index within the volume
	 * @param affinity 16-bit unsigned short (if we could do that) representing mapped affinity from [0,1]
	 */
	public void Push(int spelIndex, int affinity)
	{		
		//We also use m_pointerArray to keep track of which spels we visited
		if(m_pointerArray.containsKey(spelIndex))
			return;
		
		m_pointerArray.put(spelIndex, affinity);
		
		//Add spel to dial array
		m_arr.get(affinity).add(spelIndex);		
		
		if(affinity > m_largestAffinity)
		{
			m_largestAffinity = affinity;			
		}			
		
		m_size++;		
	}
	
	/**
	 * Returns and removes the spel with the highest affinity from the Dial cache
	 * @return spel that has the highest 16-bit unsigned affinity value
	 */
	public int Pop()
	{
		//We're done, this should never happen
		if(m_largestAffinity == -1)
			return -1;
		
		ArrayList<Integer> list = m_arr.get(m_largestAffinity);
		
		//FIFO		
		int result = list.remove(0); 
		m_size--;				
		
		if(list.size() == 0)
			UpdateLargestIndex();		
				
		return result;		
	}
	
	/**
	 * Returns whether the Dial cache already holds spelIndex somewhere
	 * @param spelIndex Index of the spel to update
	 * @return whether the Dial cache already holds spelIndex somewhere
	 */
	public boolean Contains(int spelIndex)
	{
		return m_pointerArray.containsKey(spelIndex);
	}
	
	/**
	 * Updates the position of spelIndex within the Dial cache
	 * @param spelIndex Index of the spel to update
	 * @param newAffinity new 16-bit unsigned affinity value
	 */
	public void Update(int spelIndex, int newAffinity)
	{		
		//Update entry in pointer array
		int oldAffinity = m_pointerArray.put(spelIndex, newAffinity);		
				
		//Update entry in Dial array
		ArrayList<Integer> oldList = m_arr.get(oldAffinity);
		oldList.remove((Object)spelIndex);		

		//We only ever update upwards: No need to check if we need to move m_largestAffinity down
		if(newAffinity > m_largestAffinity)
			m_largestAffinity = newAffinity;
		
		ArrayList<Integer> newList = m_arr.get(newAffinity);		
		newList.add(spelIndex);
	}
	
	/**
	 * Looks for the largest affinity value we have assigned spels to,
	 * and updates m_largestIndex to it
	 */
	private void UpdateLargestIndex()
	{			
		for(int i = m_largestAffinity; i >= 0; i--)
		{
			ArrayList<Integer> list = m_arr.get(i);
			
			if(list.size() != 0)
			{		
				m_largestAffinity = i;				
				return;
			}				
		}		
		
		m_largestAffinity = -1;		
	}
}
